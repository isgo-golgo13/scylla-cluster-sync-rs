use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

pub struct RetryPolicy {
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
    exponential_base: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            exponential_base: 2.0,
        }
    }
}

impl RetryPolicy {
    pub async fn execute<F, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> Result<T, E>,
        E: std::fmt::Display,
    {
        let mut attempt = 0;
        let mut delay = self.initial_delay;
        
        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(err) if attempt >= self.max_attempts - 1 => {
                    warn!("Operation failed after {} attempts: {}", self.max_attempts, err);
                    return Err(err);
                }
                Err(err) => {
                    warn!("Operation failed (attempt {}): {}, retrying...", attempt + 1, err);
                    sleep(delay).await;
                    delay = std::cmp::min(
                        self.max_delay,
                        Duration::from_secs_f64(delay.as_secs_f64() * self.exponential_base),
                    );
                    attempt += 1;
                }
            }
        }
    }
}
