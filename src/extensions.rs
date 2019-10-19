use std::any::{Any, TypeId};
use std::collections::HashMap;

/// A "type map" used to associate data with pooled connections.
///
/// `Extensions` is a data structure mapping types to a value of that type. This
/// can be used to, for example, cache prepared statements along side their
/// connection.
#[derive(Default)]
pub struct Extensions(HashMap<TypeId, Box<dyn Any + Sync + Send>>);

impl Extensions {
    /// Returns a new, empty `Extensions`.
    #[inline]
    pub fn new() -> Extensions {
        Extensions::default()
    }

    /// Inserts a new value into the map.
    ///
    /// Returns the previously stored value of that type, if present.
    pub fn insert<T>(&mut self, value: T) -> Option<T>
    where
        T: 'static + Sync + Send,
    {
        self.0
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(|v| Box::<dyn Any + 'static>::downcast(v).ok())
            .map(|v| *v)
    }

    /// Returns a shared reference to the stored value of the specified type.
    pub fn get<T>(&self) -> Option<&T>
    where
        T: 'static + Sync + Send,
    {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|v| v.downcast_ref())
    }

    /// Returns a mutable reference to the stored value of the specified type.
    pub fn get_mut<T>(&mut self) -> Option<&mut T>
    where
        T: 'static + Sync + Send,
    {
        self.0
            .get_mut(&TypeId::of::<T>())
            .and_then(|v| v.downcast_mut())
    }

    /// Removes the value of the specified type from the map, returning it.
    pub fn remove<T>(&mut self) -> Option<T>
    where
        T: 'static + Sync + Send,
    {
        self.0
            .remove(&TypeId::of::<T>())
            .and_then(|v| Box::<dyn Any + 'static>::downcast(v).ok())
            .map(|v| *v)
    }

    /// Removes all values from the map.
    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
    }
}
