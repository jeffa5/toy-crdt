use stateright::actor::Id;

pub(crate) type Timestamp = (u32, usize);

pub(crate) trait Map {
    fn new(actor_id: Id) -> Self;

    fn get(&self, k: &char) -> Option<&char>;

    fn set(&mut self, key: char, v: char) -> Timestamp;

    fn delete(&mut self, key: &char) -> Option<Timestamp>;

    fn receive_set(&mut self, timestamp: Timestamp, key: char, value: char);

    fn receive_delete(&mut self, timestamp: Timestamp);

    fn values(&self) -> Vec<(Timestamp, char, char)>;
}
