use command_group::AsyncGroupChild;
#[cfg(unix)]
use tokio::time::Duration;

#[cfg(target_os = "macos")]
mod macos {
    use std::{
        collections::{HashMap, HashSet},
        ffi::c_void,
        io,
        mem::MaybeUninit,
        os::raw::c_int,
    };

    use command_group::Signal;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct ProcBsdInfo {
        flags: u32,
        status: u32,
        xstatus: u32,
        pid: u32,
        ppid: u32,
        uid: u32,
        gid: u32,
        ruid: u32,
        rgid: u32,
        svuid: u32,
        svgid: u32,
        rfu_1: u32,
        comm: [i8; 16],
        name: [i8; 32],
        nfiles: u32,
        pgid: u32,
        pjobc: u32,
        e_tdev: u32,
        e_tpgid: u32,
        nice: i32,
        start_tvsec: u64,
        start_tvusec: u64,
    }

    unsafe extern "C" {
        fn proc_listpids(kind: u32, typeinfo: u32, buffer: *mut c_void, size: c_int) -> c_int;
        fn proc_pidinfo(
            pid: c_int,
            flavor: c_int,
            arg: u64,
            buffer: *mut c_void,
            size: c_int,
        ) -> c_int;
    }

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct Identity {
        pid: u32,
        started: (u64, u64),
    }

    #[derive(Clone, Copy)]
    struct Process {
        identity: Identity,
        ppid: u32,
        pgid: u32,
    }

    pub(super) struct OwnedDescendants {
        leader: Identity,
        identities: HashSet<Identity>,
        escaped_groups: Vec<u32>,
    }

    fn census() -> io::Result<HashMap<u32, Process>> {
        const PROC_ALL_PIDS: u32 = 1;
        const PROC_PIDTBSDINFO: c_int = 3;
        let required = unsafe { proc_listpids(PROC_ALL_PIDS, 0, std::ptr::null_mut(), 0) };
        if required <= 0 {
            return Err(io::Error::other("cannot size macOS process census"));
        }
        let mut pids = vec![0_i32; required as usize / size_of::<c_int>() + 64];
        let capacity = (pids.len() * size_of::<c_int>()) as c_int;
        let bytes = unsafe { proc_listpids(PROC_ALL_PIDS, 0, pids.as_mut_ptr().cast(), capacity) };
        if bytes < 0 || bytes == capacity {
            return Err(io::Error::other(
                "macOS process census unavailable or truncated",
            ));
        }
        pids.truncate(bytes as usize / size_of::<c_int>());

        let mut processes = HashMap::new();
        for pid in pids.into_iter().filter(|pid| *pid > 0) {
            let mut info = MaybeUninit::<ProcBsdInfo>::zeroed();
            let read = unsafe {
                proc_pidinfo(
                    pid,
                    PROC_PIDTBSDINFO,
                    0,
                    info.as_mut_ptr().cast(),
                    size_of::<ProcBsdInfo>() as c_int,
                )
            };
            if read != size_of::<ProcBsdInfo>() as c_int {
                let alive = unsafe { nix::libc::kill(pid, 0) } == 0
                    || io::Error::last_os_error().raw_os_error() == Some(nix::libc::EPERM);
                if alive {
                    return Err(io::Error::other(format!(
                        "cannot identify live process {pid} during macOS census"
                    )));
                }
                continue;
            }
            let info = unsafe { info.assume_init() };
            processes.insert(
                info.pid,
                Process {
                    identity: Identity {
                        pid: info.pid,
                        started: (info.start_tvsec, info.start_tvusec),
                    },
                    ppid: info.ppid,
                    pgid: info.pgid,
                },
            );
        }
        Ok(processes)
    }

    fn descendants(processes: &HashMap<u32, Process>, leader: u32) -> HashSet<Identity> {
        let mut identities = HashSet::new();
        let mut parents = HashSet::from([leader]);
        loop {
            let next: Vec<_> = processes
                .values()
                .filter(|process| {
                    parents.contains(&process.ppid) && !identities.contains(&process.identity)
                })
                .map(|process| process.identity)
                .collect();
            if next.is_empty() {
                return identities;
            }
            parents = next.iter().map(|identity| identity.pid).collect();
            identities.extend(next);
        }
    }

    impl OwnedDescendants {
        fn capture(leader_pid: u32) -> io::Result<Self> {
            let processes = census()?;
            let leader = processes
                .get(&leader_pid)
                .ok_or_else(|| io::Error::other("executor leader disappeared during census"))?;
            if leader.pgid != leader_pid {
                return Err(io::Error::other("executor leader process group changed"));
            }
            let identities = descendants(&processes, leader_pid);
            let groups: HashSet<_> = identities
                .iter()
                .filter_map(|identity| processes.get(&identity.pid))
                .map(|process| process.pgid)
                .filter(|pgid| *pgid != leader_pid)
                .collect();
            validate_groups(&processes, &identities, &groups)?;
            let mut escaped_groups: Vec<_> = groups.into_iter().collect();
            escaped_groups.sort_unstable();
            Ok(Self {
                leader: leader.identity,
                identities,
                escaped_groups,
            })
        }

        fn signal_escaped(&self, signal: Signal) -> io::Result<()> {
            let processes = census()?;
            let groups = self.escaped_groups.iter().copied().collect();
            validate_groups(&processes, &self.identities, &groups)?;
            for pgid in &self.escaped_groups {
                signal_group(*pgid, signal)?;
            }
            Ok(())
        }

        fn exited(&self) -> io::Result<bool> {
            let processes = census()?;
            Ok(std::iter::once(self.leader)
                .chain(self.identities.iter().copied())
                .all(|identity| {
                    processes
                        .get(&identity.pid)
                        .is_none_or(|process| process.identity != identity)
                }))
        }
    }

    fn validate_groups(
        processes: &HashMap<u32, Process>,
        owned: &HashSet<Identity>,
        groups: &HashSet<u32>,
    ) -> io::Result<()> {
        if let Some(process) = processes
            .values()
            .find(|process| groups.contains(&process.pgid) && !owned.contains(&process.identity))
        {
            return Err(io::Error::other(format!(
                "refusing to signal group {} containing unowned pid {}",
                process.pgid, process.identity.pid
            )));
        }
        Ok(())
    }

    fn signal_group(pgid: u32, signal: Signal) -> io::Result<()> {
        match nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pgid as c_int), signal) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
            Err(error) => Err(io::Error::from(error)),
        }
    }

    pub(super) fn capture(leader_pid: Option<u32>) -> io::Result<Option<OwnedDescendants>> {
        leader_pid.map(OwnedDescendants::capture).transpose()
    }

    pub(super) fn signal(owned: &Option<OwnedDescendants>, signal: Signal) -> io::Result<()> {
        match owned {
            Some(owned) => owned.signal_escaped(signal),
            None => Ok(()),
        }
    }

    pub(super) async fn verified(owned: Option<OwnedDescendants>) -> io::Result<bool> {
        match owned {
            Some(owned) => {
                for _ in 0..100 {
                    if owned.exited()? {
                        return Ok(true);
                    }
                    tokio::time::sleep(super::Duration::from_millis(20)).await;
                }
                Err(io::Error::other(
                    "native descendant exit could not be verified",
                ))
            }
            None => Ok(false),
        }
    }
}

/// Terminates the captured process group. On macOS, `true` additionally means
/// that descendants whose ownership was established before signalling, including
/// separate-session groups, were observed exited. `false` means containment was
/// attempted but descendant ownership or exit could not be established.
pub async fn kill_process_group(child: &mut AsyncGroupChild) -> std::io::Result<bool> {
    #[cfg(target_os = "macos")]
    let owned = macos::capture(child.inner().id());

    let mut first_error = None;
    #[cfg(target_os = "macos")]
    if let Err(error) = &owned {
        first_error = Some(std::io::Error::new(error.kind(), error.to_string()));
    }

    #[cfg(unix)]
    {
        use command_group::{Signal, UnixChildExt};
        for signal in [Signal::SIGINT, Signal::SIGTERM, Signal::SIGKILL] {
            #[cfg(target_os = "macos")]
            if let Ok(snapshot) = &owned
                && let Err(error) = macos::signal(snapshot, signal)
            {
                first_error.get_or_insert(error);
            }
            if let Err(error) = child.signal(signal)
                && error.raw_os_error() != Some(nix::libc::ESRCH)
            {
                first_error.get_or_insert(error);
            }
            if signal != Signal::SIGKILL {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    if let Err(error) = child.kill().await
        && error.kind() != std::io::ErrorKind::InvalidInput
    {
        first_error.get_or_insert(error);
    }
    if let Err(error) = child.wait().await {
        first_error.get_or_insert(error);
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    #[cfg(target_os = "macos")]
    return macos::verified(owned?).await;
    #[cfg(not(target_os = "macos"))]
    Ok(false)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use std::{process::Stdio, time::Duration};

    use command_group::AsyncCommandGroup;

    use super::kill_process_group;

    #[tokio::test]
    async fn verifies_separate_session_descendant_without_signalling_sentinel() {
        let directory = tempfile::tempdir().unwrap();
        let pid_path = directory.path().join("setsid.pid");
        let script = r#"import os,sys,time
os.setsid()
open(sys.argv[1], "w").write(str(os.getpid()))
time.sleep(30)
"#;
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .args([
                "-c",
                "'/usr/bin/python3' -c \"$0\" \"$1\" & wait",
                script,
                pid_path.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut execution = command.group_spawn().unwrap();
        let mut sentinel_command = tokio::process::Command::new("/bin/sleep");
        sentinel_command
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut sentinel = sentinel_command.group_spawn().unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            while !pid_path.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let descendant_pid = tokio::fs::read_to_string(&pid_path)
            .await
            .unwrap()
            .parse::<i32>()
            .unwrap();
        let leader_pid = execution.inner().id().unwrap() as i32;
        assert_eq!(unsafe { nix::libc::kill(descendant_pid, 0) }, 0);
        assert_ne!(unsafe { nix::libc::getpgid(descendant_pid) }, leader_pid);
        assert!(kill_process_group(&mut execution).await.unwrap());
        assert_eq!(unsafe { nix::libc::kill(descendant_pid, 0) }, -1);
        assert!(sentinel.try_wait().unwrap().is_none());
        sentinel.kill().await.unwrap();
    }

    #[tokio::test]
    async fn reaped_leader_preserves_containment_without_claiming_verification() {
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", "exit 0"]);
        let mut execution = command.group_spawn().unwrap();
        execution.wait().await.unwrap();
        assert!(!kill_process_group(&mut execution).await.unwrap());
    }
}
