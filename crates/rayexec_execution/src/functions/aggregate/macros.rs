// TODO: Document
macro_rules! generate_unary_primitive_aggregate {
    ($name:ident, $input_variant:ident, $output_variant:ident, $state:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl crate::functions::aggregate::SpecializedAggregateFunction for $name {
            fn new_grouped_state(&self) -> Box<dyn crate::functions::aggregate::GroupedStates> {
                use crate::functions::aggregate::DefaultGroupedStates;
                use rayexec_bullet::array::{Array, PrimitiveArray};
                use rayexec_bullet::executor::aggregate::{
                    StateCombiner, StateFinalizer, UnaryNonNullUpdater,
                };

                let update_fn = |row_selection: &Bitmap,
                                 arrays: &[&Array],
                                 mapping: &[usize],
                                 states: &mut [$state]| {
                    let inputs = match &arrays[0] {
                        Array::$input_variant(arr) => arr,
                        other => panic!("unexpected array type: {other:?}"),
                    };
                    UnaryNonNullUpdater::update(row_selection, inputs, mapping, states)
                };

                let finalize_fn = |states: vec::Drain<'_, _>| {
                    let mut buffer = Vec::with_capacity(states.len());
                    StateFinalizer::finalize(states, &mut buffer)?;
                    Ok(Array::$output_variant(PrimitiveArray::new(buffer, None)))
                };

                Box::new(DefaultGroupedStates::new(
                    update_fn,
                    StateCombiner::combine,
                    finalize_fn,
                ))
            }
        }
    };
}

pub(crate) use generate_unary_primitive_aggregate;

macro_rules! generate_unary_decimal_aggregate {
    ($name:ident, $variant:ident, $array:ident, $state:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name {
            pub precision: u8,
            pub scale: i8,
        }

        impl crate::functions::aggregate::SpecializedAggregateFunction for $name {
            fn new_grouped_state(&self) -> Box<dyn crate::functions::aggregate::GroupedStates> {
                use crate::functions::aggregate::DefaultGroupedStates;
                use rayexec_bullet::array::{Array, PrimitiveArray};
                use rayexec_bullet::executor::aggregate::{
                    StateCombiner, StateFinalizer, UnaryNonNullUpdater,
                };

                let update_fn = |row_selection: &Bitmap,
                                 arrays: &[&Array],
                                 mapping: &[usize],
                                 states: &mut [$state]| {
                    let inputs = match &arrays[0] {
                        Array::$variant(arr) => arr.get_primitive(),
                        other => panic!("unexpected array type: {other:?}"),
                    };
                    UnaryNonNullUpdater::update(row_selection, inputs, mapping, states)
                };

                let precision = self.precision;
                let scale = self.scale;

                let finalize_fn = move |states: vec::Drain<'_, _>| {
                    let mut buffer = Vec::with_capacity(states.len());
                    StateFinalizer::finalize(states, &mut buffer)?;
                    let primitive = PrimitiveArray::new(buffer, None);
                    Ok(Array::$variant(<$array>::new(precision, scale, primitive)))
                };

                Box::new(DefaultGroupedStates::new(
                    update_fn,
                    StateCombiner::combine,
                    finalize_fn,
                ))
            }
        }
    };
}

pub(crate) use generate_unary_decimal_aggregate;
