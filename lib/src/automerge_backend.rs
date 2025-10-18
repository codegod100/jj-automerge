#![expect(missing_docs)]

use std::fs;
use std::io::Cursor;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use automerge::transaction::Transactable as _;
use automerge::{ActorId, Automerge, ObjId, ObjType, ROOT, ReadDoc, Value};
use blake2::Blake2b512;
use blake2::Digest as _;
use futures::stream;
use futures::stream::BoxStream;
use prost::Message as _;
use tempfile::NamedTempFile;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt as _;

use crate::backend::Backend;
use crate::backend::BackendError;
use crate::backend::BackendInitError;
use crate::backend::BackendLoadError;
use crate::backend::BackendResult;
use crate::backend::ChangeId;
use crate::backend::Commit;
use crate::backend::CommitId;
use crate::backend::CopyHistory;
use crate::backend::CopyId;
use crate::backend::CopyRecord;
use crate::backend::FileId;
use crate::backend::MergedTreeId;
use crate::backend::MillisSinceEpoch;
use crate::backend::SecureSig;
use crate::backend::Signature;
use crate::backend::SigningFn;
use crate::backend::SymlinkId;
use crate::backend::Timestamp;
use crate::backend::Tree;
use crate::backend::TreeId;
use crate::backend::TreeValue;
use crate::backend::make_root_commit;
use crate::content_hash::blake2b_hash;
use crate::index::Index;
use crate::merge::MergeBuilder;
use crate::object_id::ObjectId;
use crate::repo_path::RepoPath;
use crate::repo_path::RepoPathBuf;

const DOC_FILE_NAME: &str = "automerge-store.bin";
const FILES_KEY: &str = "files";
const TREES_KEY: &str = "trees";
const COMMITS_KEY: &str = "commits";
const SYMLINKS_KEY: &str = "symlinks";
const ACTOR_ID_BYTES: &[u8] = b"jj-automerge-backend";

#[derive(Debug)]
pub struct AutomergeBackend {
    doc_path: PathBuf,
    state: Mutex<AutomergeState>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

#[derive(Debug)]
struct AutomergeState {
    doc: Automerge,
    files: ObjId,
    trees: ObjId,
    commits: ObjId,
    symlinks: ObjId,
}

impl AutomergeBackend {
    pub fn name() -> &'static str {
        "Automerge"
    }

    pub fn init(store_path: &Path) -> Result<Self, BackendInitError> {
        fs::create_dir_all(store_path).map_err(|err| BackendInitError(err.into()))?;
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::from(ACTOR_ID_BYTES));
        let state = AutomergeState::initialize(doc).map_err(|err| BackendInitError(err.into()))?;
        let backend = Self::from_state(store_path, state);
        backend
            .persist_current_doc()
            .map_err(|err| BackendInitError(err.into()))?;
        backend
            .ensure_empty_tree_present()
            .map_err(|err| BackendInitError(err.into()))?;
        Ok(backend)
    }

    pub fn load(store_path: &Path) -> Result<Self, BackendLoadError> {
        fs::create_dir_all(store_path).map_err(|err| BackendLoadError(err.into()))?;
        let doc_path = Self::doc_path_for(store_path);
        let mut doc = if doc_path.exists() {
            let bytes = fs::read(&doc_path).map_err(|err| BackendLoadError(err.into()))?;
            if bytes.is_empty() {
                Automerge::new()
            } else {
                Automerge::load(&bytes).map_err(|err| BackendLoadError(err.into()))?
            }
        } else {
            Automerge::new()
        };
        doc.set_actor(ActorId::from(ACTOR_ID_BYTES));
        let state = AutomergeState::initialize(doc).map_err(|err| BackendLoadError(err.into()))?;
        let backend = Self::from_state(store_path, state);
        backend
            .persist_current_doc()
            .map_err(|err| BackendLoadError(err.into()))?;
        backend
            .ensure_empty_tree_present()
            .map_err(|err| BackendLoadError(err.into()))?;
        Ok(backend)
    }

    fn from_state(store_path: &Path, state: AutomergeState) -> Self {
        let root_commit_id = CommitId::from_bytes(&[0u8; 64]);
        let root_change_id = ChangeId::from_bytes(&[0u8; 16]);
        let empty_tree_id = TreeId::new(blake2b_hash(&Tree::default()).to_vec());
        Self {
            doc_path: Self::doc_path_for(store_path),
            state: Mutex::new(state),
            root_commit_id,
            root_change_id,
            empty_tree_id,
        }
    }

    fn doc_path_for(store_path: &Path) -> PathBuf {
        store_path.join(DOC_FILE_NAME)
    }

    fn persist_current_doc(&self) -> BackendResult<()> {
        let state = self.state.lock().unwrap();
        self.persist_locked_doc(&state)
    }

    fn persist_locked_doc(&self, state: &AutomergeState) -> BackendResult<()> {
        let bytes = state.doc.save();
        write_atomic(&self.doc_path, &bytes).map_err(|err| BackendError::WriteObject {
            object_type: "automerge store",
            source: err.into(),
        })
    }

    fn ensure_empty_tree_present(&self) -> BackendResult<()> {
        let mut state = self.state.lock().unwrap();
        let key = self.empty_tree_id.hex();
        if state
            .has_entry(&state.trees, &key)
            .map_err(|err| BackendError::ReadObject {
                object_type: "tree".to_string(),
                hash: key.clone(),
                source: err.into(),
            })?
        {
            return Ok(());
        }
        let tree = Tree::default();
        let proto = tree_to_proto(&tree).encode_to_vec();
        let trees_obj = state.trees.clone();
        state
            .put_bytes(&trees_obj, &key, proto)
            .map_err(|err| BackendError::WriteObject {
                object_type: "tree",
                source: err.into(),
            })?;
        self.persist_locked_doc(&state)?;
        Ok(())
    }
}

#[async_trait]
impl Backend for AutomergeBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        64
    }

    fn change_id_length(&self) -> usize {
        16
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<std::pin::Pin<Box<dyn AsyncRead + Send>>> {
        let key = id.hex();
        let bytes = {
            let state = self.state.lock().unwrap();
            match state.get_bytes(&state.files, &key) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => {
                    return Err(BackendError::ObjectNotFound {
                        object_type: "file".to_string(),
                        hash: key,
                        source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found")
                            .into(),
                    });
                }
                Err(err) => {
                    return Err(BackendError::ReadFile {
                        path: path.to_owned(),
                        id: id.clone(),
                        source: err.into(),
                    });
                }
            }
        };
        Ok(Box::pin(Cursor::new(bytes)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut data = Vec::new();
        let mut hasher = Blake2b512::new();
        let mut buffer = vec![0; 1 << 15];
        loop {
            let bytes_read =
                contents
                    .read(&mut buffer)
                    .await
                    .map_err(|err| BackendError::WriteObject {
                        object_type: "file",
                        source: err.into(),
                    })?;
            if bytes_read == 0 {
                break;
            }
            let chunk = &buffer[..bytes_read];
            hasher.update(chunk);
            data.extend_from_slice(chunk);
        }
        let id = FileId::new(hasher.finalize().to_vec());
        let key = id.hex();
        {
            let mut state = self.state.lock().unwrap();
            if let Ok(Some(existing)) = state.get_bytes(&state.files, &key) {
                if existing == data {
                    return Ok(id);
                }
            }
            let files_obj = state.files.clone();
            state
                .put_bytes(&files_obj, &key, data)
                .map_err(|err| BackendError::WriteObject {
                    object_type: "file",
                    source: err.into(),
                })?;
            self.persist_locked_doc(&state)?;
        }
        Ok(id)
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let key = id.hex();
        let state = self.state.lock().unwrap();
        match state.get_string(&state.symlinks, &key) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(BackendError::ObjectNotFound {
                object_type: "symlink".to_string(),
                hash: key,
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found").into(),
            }),
            Err(err) => Err(BackendError::ReadObject {
                object_type: "symlink".to_string(),
                hash: key,
                source: err.into(),
            }),
        }
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let mut hasher = Blake2b512::new();
        hasher.update(target.as_bytes());
        let id = SymlinkId::new(hasher.finalize().to_vec());
        let key = id.hex();
        {
            let mut state = self.state.lock().unwrap();
            if let Ok(Some(existing)) = state.get_string(&state.symlinks, &key) {
                if existing == target {
                    return Ok(id);
                }
            }
            let symlinks_obj = state.symlinks.clone();
            state
                .put_string(&symlinks_obj, &key, target.to_owned())
                .map_err(|err| BackendError::WriteObject {
                    object_type: "symlink",
                    source: err.into(),
                })?;
            self.persist_locked_doc(&state)?;
        }
        Ok(id)
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "Automerge backend doesn't support copy tracking".to_string(),
        ))
    }

    async fn write_copy(&self, _copy: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "Automerge backend doesn't support copy tracking".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<CopyHistory>> {
        Err(BackendError::Unsupported(
            "Automerge backend doesn't support copy tracking".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        let key = id.hex();
        let state = self.state.lock().unwrap();
        let bytes = match state.get_bytes(&state.trees, &key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(BackendError::ObjectNotFound {
                    object_type: "tree".to_string(),
                    hash: key,
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found").into(),
                });
            }
            Err(err) => {
                return Err(BackendError::ReadObject {
                    object_type: "tree".to_string(),
                    hash: key,
                    source: err.into(),
                });
            }
        };
        let proto = crate::protos::simple_store::Tree::decode(&*bytes).map_err(|err| {
            BackendError::ReadObject {
                object_type: "tree".to_string(),
                hash: id.hex(),
                source: err.into(),
            }
        })?;
        Ok(tree_from_proto(proto))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        let id = TreeId::new(blake2b_hash(tree).to_vec());
        let key = id.hex();
        let proto = tree_to_proto(tree).encode_to_vec();
        {
            let mut state = self.state.lock().unwrap();
            if let Ok(Some(existing)) = state.get_bytes(&state.trees, &key) {
                if existing == proto {
                    return Ok(id);
                }
            }
            let trees_obj = state.trees.clone();
            state
                .put_bytes(&trees_obj, &key, proto)
                .map_err(|err| BackendError::WriteObject {
                    object_type: "tree",
                    source: err.into(),
                })?;
            self.persist_locked_doc(&state)?;
        }
        Ok(id)
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id.clone(),
                self.empty_tree_id.clone(),
            ));
        }
        let key = id.hex();
        let state = self.state.lock().unwrap();
        let bytes = match state.get_bytes(&state.commits, &key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(BackendError::ObjectNotFound {
                    object_type: "commit".to_string(),
                    hash: key,
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found").into(),
                });
            }
            Err(err) => {
                return Err(BackendError::ReadObject {
                    object_type: "commit".to_string(),
                    hash: key,
                    source: err.into(),
                });
            }
        };
        let proto = crate::protos::simple_store::Commit::decode(&*bytes).map_err(|err| {
            BackendError::ReadObject {
                object_type: "commit".to_string(),
                hash: id.hex(),
                source: err.into(),
            }
        })?;
        Ok(commit_from_proto(proto))
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.parents.is_empty() {
            return Err(BackendError::Other(
                "Cannot write a commit with no parents".into(),
            ));
        }
        let mut proto = commit_to_proto(&commit);
        if let Some(sign) = sign_with {
            let data = proto.encode_to_vec();
            let sig = sign(&data).map_err(|err| BackendError::WriteObject {
                object_type: "commit",
                source: err.into(),
            })?;
            proto.secure_sig = Some(sig.clone());
            commit.secure_sig = Some(SecureSig { data, sig });
        }
        let bytes = proto.encode_to_vec();
        let id = CommitId::new(blake2b_hash(&commit).to_vec());
        let key = id.hex();
        {
            let mut state = self.state.lock().unwrap();
            if let Ok(Some(existing)) = state.get_bytes(&state.commits, &key) {
                if existing == bytes {
                    return Ok((id, commit));
                }
            }
            let commits_obj = state.commits.clone();
            state.put_bytes(&commits_obj, &key, bytes).map_err(|err| {
                BackendError::WriteObject {
                    object_type: "commit",
                    source: err.into(),
                }
            })?;
            self.persist_locked_doc(&state)?;
        }
        Ok((id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(Box::pin(stream::empty()))
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: std::time::SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

impl AutomergeState {
    fn initialize(mut doc: Automerge) -> Result<Self, automerge::AutomergeError> {
        let files = Self::ensure_map(&mut doc, FILES_KEY)?;
        let trees = Self::ensure_map(&mut doc, TREES_KEY)?;
        let commits = Self::ensure_map(&mut doc, COMMITS_KEY)?;
        let symlinks = Self::ensure_map(&mut doc, SYMLINKS_KEY)?;
        Ok(Self {
            doc,
            files,
            trees,
            commits,
            symlinks,
        })
    }

    fn ensure_map(doc: &mut Automerge, key: &str) -> Result<ObjId, automerge::AutomergeError> {
        match doc.get(ROOT, key)? {
            Some((Value::Object(ObjType::Map), id)) => return Ok(id),
            Some((value, _)) => {
                return Err(automerge::AutomergeError::InvalidValueType {
                    expected: "map".to_string(),
                    unexpected: format!("{value:?}"),
                });
            }
            None => {}
        }
        {
            let mut tx = doc.transaction();
            tx.put_object(&ROOT, key, ObjType::Map)?;
            let _ = tx.commit();
        }
        match doc.get(ROOT, key)? {
            Some((Value::Object(ObjType::Map), id)) => Ok(id),
            Some(_) | None => Err(automerge::AutomergeError::InvalidValueType {
                expected: "map".to_string(),
                unexpected: format!("missing map for key '{key}'"),
            }),
        }
    }

    fn get_bytes(
        &self,
        obj: &ObjId,
        key: &str,
    ) -> Result<Option<Vec<u8>>, automerge::AutomergeError> {
        match self.doc.get(obj.clone(), key)? {
            Some((Value::Scalar(scalar), _)) => {
                if let automerge::ScalarValue::Bytes(bytes) = scalar.as_ref() {
                    Ok(Some(bytes.to_vec()))
                } else {
                    Err(automerge::AutomergeError::InvalidValueType {
                        expected: "bytes".to_string(),
                        unexpected: format!("{scalar:?}"),
                    })
                }
            }
            Some(_) => Err(automerge::AutomergeError::InvalidValueType {
                expected: "bytes".to_string(),
                unexpected: "non-scalar value".to_string(),
            }),
            None => Ok(None),
        }
    }

    fn put_bytes(
        &mut self,
        obj: &ObjId,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), automerge::AutomergeError> {
        let mut tx = self.doc.transaction();
        tx.put(obj, key, bytes)?;
        let _ = tx.commit();
        Ok(())
    }

    fn get_string(
        &self,
        obj: &ObjId,
        key: &str,
    ) -> Result<Option<String>, automerge::AutomergeError> {
        match self.doc.get(obj.clone(), key)? {
            Some((Value::Scalar(scalar), _)) => {
                if let automerge::ScalarValue::Str(text) = scalar.as_ref() {
                    Ok(Some(text.as_str().to_owned()))
                } else {
                    Err(automerge::AutomergeError::InvalidValueType {
                        expected: "string".to_string(),
                        unexpected: format!("{scalar:?}"),
                    })
                }
            }
            Some(_) => Err(automerge::AutomergeError::InvalidValueType {
                expected: "string".to_string(),
                unexpected: "non-scalar value".to_string(),
            }),
            None => Ok(None),
        }
    }

    fn put_string(
        &mut self,
        obj: &ObjId,
        key: &str,
        value: String,
    ) -> Result<(), automerge::AutomergeError> {
        let mut tx = self.doc.transaction();
        tx.put(obj, key, value)?;
        let _ = tx.commit();
        Ok(())
    }

    fn has_entry(&self, obj: &ObjId, key: &str) -> Result<bool, automerge::AutomergeError> {
        Ok(self.doc.get(obj.clone(), key)?.is_some())
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut temp = if let Some(parent) = path.parent() {
        NamedTempFile::new_in(parent)?
    } else {
        NamedTempFile::new()?
    };
    temp.write_all(bytes)?;
    temp.flush()?;
    temp.persist(path)?;
    Ok(())
}

fn tree_to_proto(tree: &Tree) -> crate::protos::simple_store::Tree {
    let mut proto = crate::protos::simple_store::Tree::default();
    for entry in tree.entries() {
        proto
            .entries
            .push(crate::protos::simple_store::tree::Entry {
                name: entry.name().as_internal_str().to_owned(),
                value: Some(tree_value_to_proto(entry.value())),
            });
    }
    proto
}

fn tree_from_proto(proto: crate::protos::simple_store::Tree) -> Tree {
    let entries = proto
        .entries
        .into_iter()
        .map(|proto_entry| {
            let value = tree_value_from_proto(proto_entry.value.unwrap());
            (
                crate::repo_path::RepoPathComponentBuf::new(proto_entry.name).unwrap(),
                value,
            )
        })
        .collect();
    Tree::from_sorted_entries(entries)
}

fn tree_value_to_proto(value: &TreeValue) -> crate::protos::simple_store::TreeValue {
    let mut proto = crate::protos::simple_store::TreeValue::default();
    match value {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => {
            proto.value = Some(crate::protos::simple_store::tree_value::Value::File(
                crate::protos::simple_store::tree_value::File {
                    id: id.to_bytes(),
                    executable: *executable,
                    copy_id: copy_id.to_bytes(),
                },
            ));
        }
        TreeValue::Symlink(id) => {
            proto.value = Some(crate::protos::simple_store::tree_value::Value::SymlinkId(
                id.to_bytes(),
            ));
        }
        TreeValue::Tree(id) => {
            proto.value = Some(crate::protos::simple_store::tree_value::Value::TreeId(
                id.to_bytes(),
            ));
        }
        TreeValue::GitSubmodule(_id) => {
            panic!("cannot store git submodules");
        }
    }
    proto
}

fn tree_value_from_proto(proto: crate::protos::simple_store::TreeValue) -> TreeValue {
    match proto.value.unwrap() {
        crate::protos::simple_store::tree_value::Value::TreeId(id) => {
            TreeValue::Tree(TreeId::new(id))
        }
        crate::protos::simple_store::tree_value::Value::File(
            crate::protos::simple_store::tree_value::File {
                id,
                executable,
                copy_id,
            },
        ) => TreeValue::File {
            id: FileId::new(id),
            executable,
            copy_id: CopyId::new(copy_id),
        },
        crate::protos::simple_store::tree_value::Value::SymlinkId(id) => {
            TreeValue::Symlink(SymlinkId::new(id))
        }
    }
}

fn commit_to_proto(commit: &Commit) -> crate::protos::simple_store::Commit {
    let mut proto = crate::protos::simple_store::Commit::default();
    for parent in &commit.parents {
        proto.parents.push(parent.to_bytes());
    }
    for predecessor in &commit.predecessors {
        proto.predecessors.push(predecessor.to_bytes());
    }
    match &commit.root_tree {
        MergedTreeId::Legacy(_) => {
            panic!("legacy trees are not supported");
        }
        MergedTreeId::Merge(tree_ids) => {
            proto.root_tree = tree_ids.iter().map(|id| id.to_bytes()).collect();
        }
    }
    proto.change_id = commit.change_id.to_bytes();
    proto.description = commit.description.clone();
    proto.author = Some(signature_to_proto(&commit.author));
    proto.committer = Some(signature_to_proto(&commit.committer));
    proto
}

fn commit_from_proto(mut proto: crate::protos::simple_store::Commit) -> Commit {
    let secure_sig = proto.secure_sig.take().map(|sig| SecureSig {
        data: proto.encode_to_vec(),
        sig,
    });
    let parents = proto.parents.into_iter().map(CommitId::new).collect();
    let predecessors = proto.predecessors.into_iter().map(CommitId::new).collect();
    let merge_builder: MergeBuilder<_> = proto.root_tree.into_iter().map(TreeId::new).collect();
    let root_tree = MergedTreeId::Merge(merge_builder.build());
    let change_id = ChangeId::new(proto.change_id);
    Commit {
        parents,
        predecessors,
        root_tree,
        change_id,
        description: proto.description,
        author: signature_from_proto(proto.author.unwrap_or_default()),
        committer: signature_from_proto(proto.committer.unwrap_or_default()),
        secure_sig,
    }
}

fn signature_to_proto(signature: &Signature) -> crate::protos::simple_store::commit::Signature {
    crate::protos::simple_store::commit::Signature {
        name: signature.name.clone(),
        email: signature.email.clone(),
        timestamp: Some(crate::protos::simple_store::commit::Timestamp {
            millis_since_epoch: signature.timestamp.timestamp.0,
            tz_offset: signature.timestamp.tz_offset,
        }),
    }
}

fn signature_from_proto(proto: crate::protos::simple_store::commit::Signature) -> Signature {
    let timestamp = proto.timestamp.unwrap_or_default();
    Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(timestamp.millis_since_epoch),
            tz_offset: timestamp.tz_offset,
        },
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use pollster::FutureExt as _;

    use super::*;
    use crate::tests::new_temp_dir;

    #[test]
    fn write_commit_parents() {
        let temp_dir = new_temp_dir();
        let backend = AutomergeBackend::init(temp_dir.path()).unwrap();
        let mut commit = Commit {
            parents: vec![],
            predecessors: vec![],
            root_tree: MergedTreeId::resolved(backend.empty_tree_id().clone()),
            change_id: ChangeId::from_hex("abc123"),
            description: "".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            backend.write_commit(commit, None).block_on()
        };

        commit.parents = vec![];
        assert_matches!(
            write_commit(commit.clone()),
            Err(BackendError::Other(err)) if err.to_string().contains("no parents")
        );

        commit.parents = vec![backend.root_commit_id().clone()];
        let first_id = write_commit(commit.clone()).unwrap().0;
        let first_commit = backend.read_commit(&first_id).block_on().unwrap();
        assert_eq!(first_commit, commit);

        commit.parents = vec![first_id.clone()];
        let second_id = write_commit(commit.clone()).unwrap().0;
        let second_commit = backend.read_commit(&second_id).block_on().unwrap();
        assert_eq!(second_commit, commit);

        commit.parents = vec![first_id.clone(), second_id.clone()];
        let merge_id = write_commit(commit.clone()).unwrap().0;
        let merge_commit = backend.read_commit(&merge_id).block_on().unwrap();
        assert_eq!(merge_commit, commit);

        commit.parents = vec![first_id, backend.root_commit_id().clone()];
        let root_merge_id = write_commit(commit.clone()).unwrap().0;
        let root_merge_commit = backend.read_commit(&root_merge_id).block_on().unwrap();
        assert_eq!(root_merge_commit, commit);
    }

    #[test]
    fn write_and_read_file() {
        let temp_dir = new_temp_dir();
        let backend = AutomergeBackend::init(temp_dir.path()).unwrap();
        let path = RepoPathBuf::from_internal_string("file").unwrap();
        let mut data = Cursor::new(b"hello".to_vec());
        let id = backend
            .write_file(path.as_ref(), &mut data)
            .block_on()
            .unwrap();
        let output = pollster::block_on(async {
            let mut reader = backend.read_file(path.as_ref(), &id).await.unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            buf
        });
        assert_eq!(output, b"hello");
    }

    #[test]
    fn persist_and_reload() {
        let temp_dir = new_temp_dir();
        {
            let backend = AutomergeBackend::init(temp_dir.path()).unwrap();
            let tree = Tree::default();
            backend
                .write_tree(RepoPath::root(), &tree)
                .block_on()
                .unwrap();
        }
        let backend = AutomergeBackend::load(temp_dir.path()).unwrap();
        backend
            .read_tree(RepoPath::root(), backend.empty_tree_id())
            .block_on()
            .unwrap();
    }

    fn create_signature() -> Signature {
        Signature {
            name: "Someone".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        }
    }
}
