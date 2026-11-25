//! Entry bodies larger than [`INLINE_MAX`] live as files under `<data>/blobs/`.

use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

pub const INLINE_MAX: usize = 64 * 1024;

#[derive(Clone)]
pub struct Blobs {
    dir: PathBuf,
}

impl Blobs {
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join("blobs");
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub async fn write(&self, body: &[u8]) -> std::io::Result<String> {
        let name = uuid::Uuid::new_v4().simple().to_string();
        tokio::fs::write(self.dir.join(&name), body).await?;
        Ok(name)
    }

    pub async fn read(&self, name: &str) -> std::io::Result<Vec<u8>> {
        tokio::fs::read(self.dir.join(name)).await
    }

    pub async fn remove(&self, names: impl IntoIterator<Item = String>) {
        for name in names {
            match tokio::fs::remove_file(self.dir.join(&name)).await {
                Err(err) if err.kind() != ErrorKind::NotFound => {
                    tracing::warn!("cannot delete blob {name}: {err}");
                }
                _ => {}
            }
        }
    }

    /// Deletes files no entry refers to. Files younger than an hour may still be mid-upload.
    pub async fn remove_orphans(&self, referenced: &HashSet<String>) -> std::io::Result<usize> {
        let cutoff = SystemTime::now() - Duration::from_secs(3600);
        let mut orphans = Vec::new();
        let mut dir = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            let old = entry.metadata().await?.modified()? < cutoff;
            if old && !referenced.contains(&name) {
                orphans.push(name);
            }
        }
        let count = orphans.len();
        self.remove(orphans).await;
        Ok(count)
    }
}
