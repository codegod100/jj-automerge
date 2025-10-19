use std::io::Cursor;

use jj_lib::automerge_backend::AutomergeBackend;
use jj_lib::backend::{Backend, BackendError};
use jj_lib::config::StackedConfig;
use jj_lib::object_id::ObjectId;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use tokio::io::AsyncReadExt as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory to hold the workspace.
    let workspace_dir = tempfile::tempdir()?;

    // Load default user settings. Real applications would read from ~/.jj/config.
    let settings = UserSettings::from_config(StackedConfig::with_defaults())?;

    // Initialize a brand-new repository that uses the Automerge backend.
    let (_workspace, repo) = Workspace::init_automerge(&settings, workspace_dir.path())?;

    // Grab the backend implementation so we can call its low-level APIs directly.
    let store = repo.store();
    let backend = store
        .backend_impl::<AutomergeBackend>()
        .expect("repo uses the Automerge backend");

    // Write a file object into the store.
    let file_path = RepoPathBuf::from_internal_string("demo.txt")?;
    let file_path_internal = file_path.clone().into_internal_string();
    let mut contents = Cursor::new(b"Hello from Automerge!\n".to_vec());
    let file_id =
        pollster::block_on(async { backend.write_file(file_path.as_ref(), &mut contents).await })?;

    // Read the file back out to show that it round-trips via the Automerge document.
    let round_tripped = pollster::block_on(async {
        let mut reader = backend.read_file(file_path.as_ref(), &file_id).await?;
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|err| BackendError::Other(err.into()))?;
        Ok::<_, BackendError>(buffer)
    })?;

    println!(
        "Stored `{}` as file id {}",
        file_path_internal,
        file_id.hex()
    );
    println!(
        "Round-tripped contents: {}",
        String::from_utf8_lossy(&round_tripped)
    );

    Ok(())
}
