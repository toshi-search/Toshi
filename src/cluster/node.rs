use std::path::Path;
use std::time;

use futures::Future;
use num_cpus;
use systemstat;
use systemstat::{Platform, System};
use tokio::{
    fs::File,
    io::{read_to_end, write_all},
};

use cluster::{ClusterError, DiskType};

static NODE_ID_FILENAME: &'static str = ".node_id";

/// Write node id to the path `p` provided, this will also append `.node_id`
pub fn write_node_id(p: String, id: String) -> impl Future<Item = String, Error = ClusterError> {
    // Append .node_id to the path provided
    let path = Path::new(&p).join(&NODE_ID_FILENAME);
    // Create and write the id to the file and return the id
    File::create(path)
        .and_then(move |file| write_all(file, id))
        .map(|(_, id)| id)
        .map_err(|e| ClusterError::FailedWritingNodeID(format!("{}", e)))
}

/// Read the node id from the file provided
///
/// Note:This function will try and Read the file as UTF-8
pub fn read_node_id(p: &str) -> impl Future<Item = String, Error = ClusterError> {
    // Append .node_id to the provided path
    let path = Path::new(p).join(&NODE_ID_FILENAME);

    // Open an read the string to the end of the file and try to read it as UTF-8
    File::open(path)
        .and_then(|file| read_to_end(file, Vec::new()))
        .map_err(|e| ClusterError::FailedReadingNodeID(format!("{}", e)))
        .and_then(|(_, bytes)| String::from_utf8(bytes).map_err(|_| ClusterError::UnableToReadUTF8))
}

/// Collection of all the metadata we can gather about the node. It is composed of
/// sub-structs, listed below. This will be serialized to JSON and sent to Consul.
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    cpu:         Result<CPUMetadata, ClusterError>,
    ram:         Result<RAMMetadata, ClusterError>,
    disks:       Result<Vec<DiskMetadata>, ClusterError>,
    directories: Result<Vec<DirectoryMetadata>, ClusterError>,
    start_time:  Option<time::SystemTime>,
    end_time:    Option<time::SystemTime>,
    duration:    Option<time::Duration>,
}

impl Metadata {
    /// Gathers metadata from all the subsystems. Accepts a list of block devices or mount points
    /// about which to gather data. Note that the directories must be mountpoints (visible in mount)
    /// not just any arbitrary directory.
    pub fn gather(block_devices: Vec<&str>, directories: Vec<&str>) -> Metadata {
        let sys = systemstat::System::new();
        let start_time = time::SystemTime::now();
        let mut metadata = Metadata {
            cpu:         CPUMetadata::gather(&sys),
            ram:         RAMMetadata::gather(&sys),
            disks:       block_devices.iter().map(|d| DiskMetadata::gather(d, &sys)).collect(),
            directories: directories.iter().map(|d| DirectoryMetadata::gather(d, &sys)).collect(),
            start_time:  None,
            end_time:    None,
            duration:    None,
        };
        let end_time = time::SystemTime::now();
        let duration = start_time.duration_since(start_time).unwrap_or_else(|_| time::Duration::new(0, 0));
        metadata.start_time = Some(start_time);
        metadata.end_time = Some(end_time);
        metadata.duration = Some(duration);
        metadata
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
    physical:              usize,
    logical:               usize,
    five_min_load_average: f32,
}

impl CPUMetadata {
    /// Gathers metadata about the CPU of the system
    pub fn gather(sys: &systemstat::System) -> Result<CPUMetadata, ClusterError> {
        match sys.load_average() {
            Ok(avg) => Ok(CPUMetadata {
                logical:               num_cpus::get(),
                physical:              num_cpus::get_physical(),
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
    free:  usize,
    used:  usize,
}

impl RAMMetadata {
    /// Gathers RAM metadata about the system it is running on
    pub fn gather(sys: &systemstat::System) -> Result<RAMMetadata, ClusterError> {
        match sys.memory() {
            Ok(mem) => Ok(RAMMetadata {
                total: mem.total.as_usize(),
                free:  mem.free.as_usize(),
                used:  (mem.total - mem.free).as_usize(),
            }),
            Err(e) => Err(ClusterError::FailedGettingRAMMetadata(e.to_string())),
        }
    }
}

/// Metadata about the block devices on the node
#[derive(Debug, Serialize, Deserialize)]
pub struct DiskMetadata {
    disk_type:       Option<DiskType>,
    write_wait_time: usize,
    read_wait_time:  usize,
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
                            read_wait_time:  blkstats.read_ticks,
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
    directory:     String,
    max_size:      usize,
    current_usage: usize,
    free_space:    usize,
}

impl DirectoryMetadata {
    /// Gathers metadata about a specific directory
    pub fn gather(filesystem_path: &str, sys: &System) -> Result<DirectoryMetadata, ClusterError> {
        match sys.mounts() {
            Ok(mounts) => {
                for mount in &mounts {
                    if mount.fs_mounted_on == filesystem_path {
                        return Ok(DirectoryMetadata {
                            directory:     mount.fs_mounted_on.clone(),
                            max_size:      mount.total.as_usize(),
                            current_usage: (mount.total - mount.avail).as_usize(),
                            free_space:    mount.free.as_usize(),
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
        if cfg!(target_os = "windows") {
            println!("Test not supported on macos yet");
            return;
        }
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
