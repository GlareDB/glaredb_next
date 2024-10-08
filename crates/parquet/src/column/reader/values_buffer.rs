use super::DataType;

pub trait ValuesBuffer<T: DataType> {
    fn len(&self) -> usize {
        unimplemented!()
    }

    fn swap(&mut self, a: usize, b: usize) {
        unimplemented!()
    }
}

impl<T> ValuesBuffer<T> for Vec<T::T> where T: DataType {}
