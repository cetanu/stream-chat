use crate::client::state::{AppState, save_client_buffer};
use crossterm::event::{self, Event, KeyCode};
use ratatui::{
    Terminal,
    backend::Backend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, Paragraph},
};
use std::time::Duration;

pub async fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    state: &mut AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        terminal.draw(|f| ui(f, state))?;

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => {
                        return Ok(());
                    }
                    KeyCode::Char(' ') | KeyCode::Char('a') | KeyCode::Char('A') => {
                        acknowledge_message(state).await;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn acknowledge_message(state: &mut AppState) {
    let limit = state.max_messages;
    let len = {
        let mut buf = state.message_buffer.lock().unwrap();
        buf.pop_front();
        if let Err(e) = save_client_buffer(&buf) {
            eprintln!("[Client] Error saving buffer: {:?}", e);
        }
        buf.len()
    };
    if len <= 2 * limit {
        let _ = state.fetch_trigger.try_send(());
    }
}

fn ui(f: &mut ratatui::Frame, state: &mut AppState) {
    let size = f.size();
    let limit = state.max_messages;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(limit as u16), Constraint::Length(1)])
        .split(size);

    let buffer_msgs = state.message_buffer.lock().unwrap().clone();
    let mut list_items = Vec::new();
    let display_count = std::cmp::min(limit, buffer_msgs.len());

    for i in (0..display_count).rev() {
        let msg = &buffer_msgs[i];
        let platform_tag = if msg.platform == "YouTube" {
            Span::styled(
                " [YouTube] ",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(
                " [Twitch]  ",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            )
        };

        let sender = Span::styled(
            format!("{}: ", msg.sender),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );

        let content = Span::styled(&msg.content, Style::default().fg(Color::Gray));

        let (marker, style) = match i {
            0 => (
                Span::styled(
                    "▶",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Style::default().bg(Color::Rgb(40, 40, 20)),
            ),
            _ => (Span::raw(" "), Style::default()),
        };

        list_items.push(
            ListItem::new(Line::from(vec![marker, platform_tag, sender, content])).style(style),
        );
    }

    let chat_list = List::new(list_items).block(Block::default());
    f.render_widget(chat_list, chunks[0]);

    // Status Block
    let (connected, last_err) = {
        let s = state.status.lock().unwrap();
        (s.connected, s.last_error.clone())
    };
    let color = match connected {
        true => Color::Green,
        false => Color::Red,
    };
    let conn_status = Span::styled("◉", Style::default().fg(color).add_modifier(Modifier::BOLD));
    let err_msg = match last_err {
        Some(e) => format!(" | Error: {} ", e),
        None => " ".to_string(),
    };

    let status_text = Line::from(vec![
        conn_status,
        Span::styled(err_msg, Style::default().fg(Color::Red)),
    ]);

    let status_block = Paragraph::new(status_text)
        .block(Block::default())
        .alignment(Alignment::Right);
    f.render_widget(status_block, chunks[1]);
}
