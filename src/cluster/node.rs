use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use num_cpus;
use systemstat;
use systemstat::{Platform, System};

use cluster::{ClusterError, DiskType};

static NODE_ID_FILENAME: &'static str = ".node_id.txt";
static CLUSTER_NAME_FILENAME: &'static str = ".cluster_name.txt";

pub fn write_node_id(id: String, _p: &str) -> Result<(), ClusterError> {
    let path = Path::new(&NODE_ID_FILENAME);
    match File::create(path) {
        Ok(_) => Ok(()),
        Err(e) => Err(ClusterError::FailedWritingNodeID(e.to_string())),
    }
}

pub fn read_node_id(p: &str) -> Result<String, ClusterError> {
    let path = NODE_ID_FILENAME;
    let path = Path::new(&path);
    let mut contents = String::new();
    let mut handle = File::open(&path).map_err(|e| ClusterError::FailedReadingNodeID(e.to_string()))?;
    handle
        .read_to_string(&mut contents)
        .map_err(|e| ClusterError::FailedReadingNodeID(e.to_string()))?;
    Ok(contents)
}

/// Collection of all the metadata we can gather about the node. It is composed of
/// sub-structs, listed below. This will be serialized to JSON and sent to Consul.
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    cpu: Result<CPUMetadata, ClusterError>,
    ram: Result<RAMMetadata, ClusterError>,
    disks: Result<Vec<DiskMetadata>, ClusterError>,
    directories: Result<Vec<DirectoryMetadata>, ClusterError>,
}

impl Metadata {
    /// Gathers metadata from all the subsystems. Accepts a list of block devices or mount points
    /// about which to gather data. Note that the directories must be mountpoints (visible in mount)
    /// not just any arbitrary directory.
    pub fn gather(block_devices: Vec<&str>, directories: Vec<&str>) -> Metadata {
        let sys = systemstat::System::new();
        Metadata {
            cpu: CPUMetadata::gather(&sys),
            ram: RAMMetadata::gather(&sys),
            disks: block_devices.iter().map(|d| DiskMetadata::gather(d, &sys)).collect(),
            directories: directories.iter().map(|d| DirectoryMetadata::gather(d, &sys)).collect(),
        }
    }
}

/// All network data about the node
#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkMetadata {
    ipv4: String,
    ipv6: String,
    port: u16,
}

/// CPU data about the node. Uses the load averages calculated by the kernel.
#[derive(Debug, Serialize, Deserialize)]
pub struct CPUMetadata {
    physical: usize,
    logical: usize,
    five_min_load_average: f32,
}

impl CPUMetadata {
    /// Gathers metadata about the CPU of the system
    pub fn gather(sys: &systemstat::System) -> Result<CPUMetadata, ClusterError> {
        match sys.load_average() {
            Ok(avg) => Ok(CPUMetadata {
                logical: num_cpus::get(),
                physical: num_cpus::get_physical(),
                five_min_load_average: avg.five,
            }),
            Err(e) => Err(ClusterError::FailedGettingCPUMetadata(e.to_string())),
        }
    }
}

/// Metadata about the node's RAM
#[derive(Debug, Serialize, Deserialize)]
pub struct RAMMetadata {
    total: usize,
    free: usize,
    used: usize,
}

impl RAMMetadata {
    /// Gathers RAM metadata about the system it is running on
    pub fn gather(sys: &systemstat::System) -> Result<RAMMetadata, ClusterError> {
        match sys.memory() {
            Ok(mem) => Ok(RAMMetadata {
                total: mem.total.as_usize(),
                free: mem.free.as_usize(),
                used: (mem.total - mem.free).as_usize(),
            }),
            Err(e) => Err(ClusterError::FailedGettingRAMMetadata(e.to_string())),
        }
    }
}

/// Metadata about the block devices on the node
#[derive(Debug, Serialize, Deserialize)]
pub struct DiskMetadata {
    disk_type: Option<DiskType>,
    write_wait_time: usize,
    read_wait_time: usize,
}

impl DiskMetadata {
    /// Gathers metadata about a specific block device
    pub fn gather(block_device_name: &str, sys: &System) -> Result<DiskMetadata, ClusterError> {
        match sys.block_device_statistics() {
            Ok(stats) => {
                for blkstats in stats.values() {
                    if block_device_name == blkstats.name {
                        return Ok(DiskMetadata {
                            // read and write wait time are in ms
                            write_wait_time: blkstats.write_ticks,
                            read_wait_time: blkstats.read_ticks,
                            // Currently do not have a good way to detect HDD or SSD, so this defaults to None for now
                            disk_type: None,
                        });
                    }
                }
                Err(ClusterError::NoMatchingBlockDeviceFound(block_device_name.into()))
            }
            Err(e) => Err(ClusterError::FailedGettingBlockDeviceMetadata(e.to_string())),
        }
    }
}

/// Metadata about the directories on the node
#[derive(Debug, Serialize, Deserialize)]
pub struct DirectoryMetadata {
    directory: String,
    max_size: usize,
    current_usage: usize,
    free_space: usize,
}

impl DirectoryMetadata {
    /// Gathers metadata about a specific directory
    pub fn gather(filesystem_path: &str, sys: &System) -> Result<DirectoryMetadata, ClusterError> {
        match sys.mounts() {
            Ok(mounts) => {
                for mount in &mounts {
                    if mount.fs_mounted_on == filesystem_path {
                        return Ok(DirectoryMetadata {
                            directory: mount.fs_mounted_on.clone(),
                            max_size: mount.total.as_usize(),
                            current_usage: (mount.total - mount.avail).as_usize(),
                            free_space: mount.free.as_usize(),
                        });
                    }
                }
                Err(ClusterError::NoMatchingDirectoryFound(filesystem_path.into()))
            }
            Err(e) => Err(ClusterError::FailedGettingDirectoryMetadata(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_cpu_metadata() {
        let sys = systemstat::System::new();
        let cpu_metadata = CPUMetadata::gather(&sys);
        assert!(cpu_metadata.is_ok())
    }

    #[test]
    fn test_ram_metadata() {
        if cfg!(target_os = "macos") {
            println!("Test not supported on macos yet");
            return;
        }
        let sys = systemstat::System::new();
        let ram_metadata = RAMMetadata::gather(&sys);
        assert!(ram_metadata.is_ok())
    }
}
