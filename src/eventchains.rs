use hashbrown::HashMap;
use std::any::Any;
use std::fmt;

/// Result of an event execution
#[derive(Debug, Clone)]
pub enum EventResult<T> {
    Success(T),
    Failure(String),
}

impl<T> EventResult<T> {
    pub fn is_success(&self) -> bool {
        matches!(self, EventResult::Success(_))
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, EventResult::Failure(_))
    }

    pub fn get_data(self) -> Option<T> {
        match self {
            EventResult::Success(data) => Some(data),
            EventResult::Failure(_) => None,
        }
    }

    pub fn get_error(&self) -> Option<&str> {
        match self {
            EventResult::Success(_) => None,
            EventResult::Failure(msg) => Some(msg),
        }
    }
}

/// Context that flows through the event chain
pub struct EventContext {
    data: HashMap<String, Box<dyn Any + Send + Sync>>,
}

impl EventContext {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn set<T: Any + Send + Sync>(&mut self, key: &str, value: T) {
        self.data.insert(key.to_string(), Box::new(value));
    }

    pub fn get<T: Any + Send + Sync + Clone>(&self, key: &str) -> Option<&T> {
        self.data
            .get(key)
            .and_then(|boxed| boxed.downcast_ref::<T>())
    }

    pub fn take<T: Any + Send + Sync>(&mut self, key: &str) -> Option<Box<T>> {
        self.data
            .remove(key)
            .map(|v| v.downcast::<T>().ok())
            .flatten()
    }

    pub fn has(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }
}

impl Default for EventContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for chainable events
pub trait ChainableEvent: Send + Sync {
    fn execute(&self, context: &mut EventContext) -> EventResult<()>;
    fn name(&self) -> &str;
}

/// Trait for middleware
pub trait EventMiddleware: Send + Sync {
    fn execute(
        &self,
        event: &dyn ChainableEvent,
        context: &mut EventContext,
        next: &mut dyn FnMut(&mut EventContext) -> EventResult<()>,
    ) -> EventResult<()>;
}

/// Event failure information
#[derive(Debug, Clone)]
pub struct EventFailure {
    pub event_name: String,
    pub error_message: String,
    pub timestamp: u64,
}

impl EventFailure {
    pub fn new(event_name: String, error_message: String) -> Self {
        Self {
            event_name,
            error_message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Chain execution result
#[derive(Debug)]
pub struct ChainResult {
    pub success: bool,
    pub failures: Vec<EventFailure>,
    pub status: ChainStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainStatus {
    Completed,
    CompletedWithWarnings,
    Failed,
}

impl ChainResult {
    pub fn success() -> Self {
        Self {
            success: true,
            failures: Vec::new(),
            status: ChainStatus::Completed,
        }
    }

    pub fn partial_success(failures: Vec<EventFailure>) -> Self {
        Self {
            success: true,
            failures,
            status: ChainStatus::CompletedWithWarnings,
        }
    }

    pub fn failure(failures: Vec<EventFailure>) -> Self {
        Self {
            success: false,
            failures,
            status: ChainStatus::Failed,
        }
    }
}

/// Fault tolerance mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultToleranceMode {
    Strict,
    Lenient,
    BestEffort,
}

/// Main EventChain orchestrator
pub struct EventChain<'a> {
    events: Vec<&'a dyn ChainableEvent>,
    middlewares: Vec<&'a dyn EventMiddleware>,
    fault_tolerance: FaultToleranceMode,
}

impl<'a> EventChain<'a> {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            middlewares: Vec::new(),
            fault_tolerance: FaultToleranceMode::Strict,
        }
    }

    pub fn with_fault_tolerance(mut self, mode: FaultToleranceMode) -> Self {
        self.fault_tolerance = mode;
        self
    }

    pub fn add_event(&mut self, event: &'a dyn ChainableEvent) -> &mut Self {
        self.events.push(event);
        self
    }

    pub fn use_middleware(&mut self, middleware: &'a dyn EventMiddleware) -> &mut Self {
        self.middlewares.push(middleware);
        self
    }

    pub fn execute(&self, context: &mut EventContext) -> ChainResult {
        let mut failures = Vec::new();

        for event in &self.events {
            // Build middleware pipeline (LIFO - last registered executes first)
            let result = self.execute_with_middleware(*event, context);

            if result.is_failure() {
                let failure = EventFailure::new(
                    event.name().to_string(),
                    result.get_error().unwrap_or("Unknown error").to_string(),
                );
                failures.push(failure);

                match self.fault_tolerance {
                    FaultToleranceMode::Strict => {
                        return ChainResult::failure(failures);
                    }
                    FaultToleranceMode::Lenient | FaultToleranceMode::BestEffort => {
                        // Continue execution
                        continue;
                    }
                }
            }
        }

        if failures.is_empty() {
            ChainResult::success()
        } else {
            ChainResult::partial_success(failures)
        }
    }

    fn execute_with_middleware(
        &self,
        event: &dyn ChainableEvent,
        context: &mut EventContext,
    ) -> EventResult<()> {
        if self.middlewares.is_empty() {
            return event.execute(context);
        }

        // Execute middleware in reverse order by recursively building the call stack
        self.execute_middleware_recursive(0, event, context)
    }

    fn execute_middleware_recursive(
        &self,
        middleware_index: usize,
        event: &dyn ChainableEvent,
        context: &mut EventContext,
    ) -> EventResult<()> {
        if middleware_index >= self.middlewares.len() {
            // Base case: execute the actual event
            return event.execute(context);
        }

        // Get the current middleware (reverse order)
        let middleware_idx = self.middlewares.len() - 1 - middleware_index;
        let middleware = &self.middlewares[middleware_idx];

        // Create a closure that calls the next middleware (or event)
        let mut next = |ctx: &mut EventContext| -> EventResult<()> {
            self.execute_middleware_recursive(middleware_index + 1, event, ctx)
        };

        // Execute this middleware with the next closure
        middleware.execute(event, context, &mut next)
    }
}

impl<'a> Default for EventChain<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ChainStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainStatus::Completed => write!(f, "COMPLETED"),
            ChainStatus::CompletedWithWarnings => write!(f, "COMPLETED_WITH_WARNINGS"),
            ChainStatus::Failed => write!(f, "FAILED"),
        }
    }
}
