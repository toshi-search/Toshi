use raft::prelude::*;

#[derive(Debug, Clone, Default)]
pub struct SledRaftState {
    pub hard_state: HardState,
    pub conf_state: ConfState,
    pub pending_conf_state: Option<ConfState>,
    pub pending_conf_state_start_index: Option<u64>,
}

impl SledRaftState {
    pub fn new(hard_state: HardState, conf_state: ConfState) -> SledRaftState {
        SledRaftState {
            hard_state,
            conf_state,
            pending_conf_state: None,
            pending_conf_state_start_index: None,
        }
    }
    pub fn initialized(&self) -> bool {
        self.conf_state != ConfState::default()
    }
}
