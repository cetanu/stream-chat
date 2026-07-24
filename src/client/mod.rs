pub mod config;
pub mod service;
pub mod state;
pub mod ui;

use crate::client::config::ClientConfig;
use crate::client::service::ServerChatStream;
use crate::client::state::{AppState, ConnectionStatus, load_client_buffer};
use crate::client::ui::run_app;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub struct StreamChatClient {
    config: ClientConfig,
}

impl StreamChatClient {
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let limit = self.config.limit;
        let max_buffer = 3 * limit;
        println!(
            "[Client] Connecting to Stream Chat Server at {}... (Displaying N={}, pre-fetching max={})",
            self.config.address, limit, max_buffer
        );

        let buffer = Arc::new(Mutex::new(load_client_buffer()));
        let status = Arc::new(Mutex::new(ConnectionStatus {
            connected: false,
            last_error: None,
            youtube_status: None,
        }));
        let (trigger_tx, trigger_rx) = mpsc::channel::<()>(10);

        // Setup TUI
        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let address = self.config.address.clone();
        let connection =
            ServerChatStream::start(buffer.clone(), status.clone(), trigger_rx, limit, address);

        let mut state = AppState {
            message_buffer: buffer,
            status,
            fetch_trigger: trigger_tx,
            max_messages: limit,
        };

        // Main event and rendering loop
        let res = run_app(&mut terminal, &mut state).await;

        // Restore terminal
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        connection.abort();

        if let Err(err) = res {
            println!("[Client Error] {:?}", err);
        }

        Ok(())
    }
}
