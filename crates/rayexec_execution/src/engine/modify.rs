
use crossbeam::channel::{unbounded, Receiver, Sender};
use rayexec_bullet::scalar::OwnedScalarValue;


/// Modifications to be applied to the session.
#[derive(Debug)]
pub enum Modification {
    UpdateVariable {
        name: String,
        value: OwnedScalarValue,
    },
    UpdateTransactionState(()),
    UpdateCatalog(()),
}

#[derive(Debug)]
pub struct SessionModifier {
    send: Sender<Modification>,
    recv: Receiver<Modification>,
}

impl Default for SessionModifier {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionModifier {
    pub fn new() -> Self {
        let (send, recv) = unbounded();
        SessionModifier { send, recv }
    }

    pub fn clone_sender(&self) -> Sender<Modification> {
        self.send.clone()
    }

    pub fn get_recv(&self) -> &Receiver<Modification> {
        &self.recv
    }
}
