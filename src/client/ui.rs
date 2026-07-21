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

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
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

    for (i, msg) in buffer_msgs.iter().enumerate().take(display_count) {
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

        let sender_str = format!("{}: ", msg.sender);
        let prefix_width = 1 + 11 + sender_str.chars().count();
        let total_width = chunks[0].width as usize;

        // Determine if we have enough space to indent subsequent lines
        let (first_line_width, subsequent_line_width, indent_len) = if total_width > prefix_width + 10 {
            (total_width - prefix_width, total_width - prefix_width, prefix_width)
        } else if total_width > 4 + 10 {
            (
                if total_width > prefix_width { total_width - prefix_width } else { 1 },
                total_width - 4,
                4
            )
        } else {
            (
                if total_width > prefix_width { total_width - prefix_width } else { 1 },
                if total_width > 0 { total_width } else { 1 },
                0
            )
        };

        let wrapped_contents = wrap_text(&msg.content, first_line_width, subsequent_line_width);
        let mut item_lines = Vec::new();

        let first_content = wrapped_contents.first().cloned().unwrap_or_default();
        let content_span = Span::styled(first_content, Style::default().fg(Color::Gray));

        item_lines.push(Line::from(vec![marker, platform_tag, sender, content_span]));

        if wrapped_contents.len() > 1 {
            let indent_str = " ".repeat(indent_len);
            for part in wrapped_contents.iter().skip(1) {
                let indent_span = Span::raw(indent_str.clone());
                let part_span = Span::styled(part.clone(), Style::default().fg(Color::Gray));
                item_lines.push(Line::from(vec![indent_span, part_span]));
            }
        }

        list_items.push(ListItem::new(item_lines).style(style));
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

fn wrap_text(text: &str, first_line_width: usize, subsequent_line_width: usize) -> Vec<String> {
    let first_line_width = std::cmp::max(first_line_width, 1);
    let subsequent_line_width = std::cmp::max(subsequent_line_width, 1);

    if text.is_empty() {
        return vec![String::new()];
    }

    let mut lines = Vec::new();
    let mut current_line = String::new();
    let mut target_width = first_line_width;

    let words: Vec<&str> = text.split(' ').collect();

    for word in words {
        let word_len = word.chars().count();
        let space_needed = if current_line.is_empty() { 0 } else { 1 };
        let current_len = current_line.chars().count();

        if current_len + space_needed + word_len <= target_width {
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(word);
        } else {
            if !current_line.is_empty() {
                lines.push(current_line);
                current_line = String::new();
            }
            target_width = subsequent_line_width;

            if word_len <= target_width {
                current_line = word.to_string();
            } else {
                let word_chars: Vec<char> = word.chars().collect();
                let mut start = 0;
                while start < word_chars.len() {
                    let end = std::cmp::min(start + target_width, word_chars.len());
                    let chunk: String = word_chars[start..end].iter().collect();
                    if end < word_chars.len() {
                        lines.push(chunk);
                        target_width = subsequent_line_width;
                    } else {
                        current_line = chunk;
                    }
                    start = end;
                }
            }
        }
    }

    if !current_line.is_empty() {
        lines.push(current_line);
    }

    if lines.is_empty() {
        lines.push(String::new());
    }

    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_text_basic() {
        let result = wrap_text("hello world", 10, 10);
        assert_eq!(result, vec!["hello", "world"]);
    }

    #[test]
    fn test_wrap_text_long_word() {
        let result = wrap_text("supercalifragilistic", 5, 5);
        assert_eq!(result, vec!["super", "calif", "ragil", "istic"]);
    }

    #[test]
    fn test_wrap_text_different_widths() {
        let result = wrap_text("hello wonderful world", 10, 5);
        assert_eq!(result, vec!["hello", "wonde", "rful", "world"]);
    }

    #[test]
    fn test_wrap_text_spaces() {
        let result = wrap_text("a  b", 10, 10);
        assert_eq!(result, vec!["a  b"]);
    }

    #[test]
    fn test_wrap_text_empty() {
        let result = wrap_text("", 5, 5);
        assert_eq!(result, vec![""]);
    }
}
