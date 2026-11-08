//! Retained originals, organized by sender beside a read-only conversation.

use std::collections::HashMap;

use egui::{Align, Align2, Frame, Layout, Margin, Rect, RichText, Sense, vec2};

use crate::app::App;
use crate::backend::DeletedMessage;
use crate::capture::view::{SavedAttachment, SavedObservation};
use crate::i18n::Locale;
use crate::model::{Action, Page};
use crate::theme::{self, Icon, Palette};

use super::widgets;

const LIST_WIDTH: f32 = 300.0;
const ROW_HEIGHT: f32 = 72.0;
pub const NARROW_WIDTH: f32 = 700.0;

pub fn search_id() -> egui::Id {
    egui::Id::new("deleted-messages-search")
}

pub fn sender_row_id(account: &str, sender: &str) -> egui::Id {
    egui::Id::new(("deleted-sender", account, sender))
}

struct SenderGroup {
    key: String,
    name: String,
    rows: Vec<usize>,
    latest: usize,
    latest_sequence: i64,
    matching: bool,
}

pub fn show(app: &mut App, ui: &mut egui::Ui) {
    let narrow = ui.ctx().content_rect().width() < NARROW_WIDTH;
    let palette = app.palette;
    let mut groups = sender_groups(app);
    let mut selected = app.deleted_selected_sender.clone();
    keep_visible_selection(&groups, &mut selected);
    let mut show_thread = narrow && app.deleted_show_thread && selected.is_some();
    let mut actions = Vec::new();
    if !narrow {
        let panel = egui::Panel::left("deleted-sender-list")
            .resizable(false)
            .exact_size(LIST_WIDTH)
            .show_separator_line(false)
            .frame(
                Frame::new()
                    .fill(palette.sidebar())
                    .inner_margin(Margin::ZERO),
            )
            .show(ui, |ui| {
                sender_list(
                    app,
                    ui,
                    false,
                    &mut groups,
                    &mut selected,
                    &mut show_thread,
                    &mut actions,
                );
            });
        widgets::paint_edge_beside(ui, &palette, panel.response.rect);
        thread(app, ui, &groups, selected.as_deref(), false, &mut actions);
    } else if show_thread {
        thread(app, ui, &groups, selected.as_deref(), true, &mut actions);
    } else {
        Frame::new()
            .fill(palette.sidebar())
            .inner_margin(Margin::ZERO)
            .show(ui, |ui| {
                ui.set_min_size(ui.available_size());
                sender_list(
                    app,
                    ui,
                    true,
                    &mut groups,
                    &mut selected,
                    &mut show_thread,
                    &mut actions,
                );
            });
    }
    app.deleted_selected_sender = selected;
    app.deleted_show_thread = show_thread;
    app.actions.extend(actions);
}

/// Privacy filtering precedes grouping, counts, previews, and search.
fn sender_groups(app: &App) -> Vec<SenderGroup> {
    let query = app.deleted_query.trim().to_lowercase();
    let mut groups: Vec<SenderGroup> = Vec::new();
    let mut positions = HashMap::<String, usize>::new();
    for (index, row) in app.deleted_messages.iter().enumerate() {
        if !app
            .chat(&row.canonical_chat)
            .is_some_and(|chat| !chat.locked)
            || !matches_query(row, &query)
        {
            continue;
        }
        let key = sender_key(
            &row.sender_id,
            row.saved.outgoing,
            &row.canonical_chat,
            &row.saved.message_id,
        );
        if let Some(position) = positions.get(&key).copied() {
            let group = &mut groups[position];
            group.rows.push(index);
            if row.saved.sequence > group.latest_sequence {
                group.latest = index;
                group.latest_sequence = row.saved.sequence;
                group.name = sender_name(row).to_owned();
            }
        } else {
            positions.insert(key.clone(), groups.len());
            groups.push(SenderGroup {
                key,
                name: sender_name(row).to_owned(),
                rows: vec![index],
                latest: index,
                latest_sequence: row.saved.sequence,
                matching: !query.is_empty(),
            });
        }
    }
    for group in &mut groups {
        group
            .rows
            .sort_by_key(|&index| app.deleted_messages[index].saved.sequence);
    }
    groups.sort_by(|a, b| {
        b.latest_sequence
            .cmp(&a.latest_sequence)
            .then_with(|| a.key.cmp(&b.key))
    });
    groups
}

fn sender_key(sender: &str, outgoing: bool, chat: &str, message: &str) -> String {
    if outgoing {
        "wassup:own-messages".to_owned()
    } else if sender.trim().is_empty() {
        format!("wassup:unknown-sender:{chat}:{message}")
    } else {
        sender.to_owned()
    }
}

fn sender_name(row: &DeletedMessage) -> &str {
    if row.saved.outgoing {
        "You"
    } else if row.sender_name.is_empty() {
        "Unknown sender"
    } else {
        &row.sender_name
    }
}

fn keep_visible_selection(groups: &[SenderGroup], selected: &mut Option<String>) {
    if !groups
        .iter()
        .any(|group| Some(&group.key) == selected.as_ref())
    {
        *selected = groups.first().map(|group| group.key.clone());
    }
}

fn sender_list(
    app: &mut App,
    ui: &mut egui::Ui,
    narrow: bool,
    groups: &mut Vec<SenderGroup>,
    selected: &mut Option<String>,
    show_thread: &mut bool,
    actions: &mut Vec<Action>,
) {
    let palette = app.palette;
    // Reserve the footer now, but register its controls after the list so
    // native Tab order follows the visible header, senders, and footer.
    let footer_line = ui
        .painter()
        .layout_no_wrap(
            "Search covers loaded messages.".to_owned(),
            theme::regular(11.5),
            palette.secondary,
        )
        .size()
        .y;
    let footer_height = 24.0
        + footer_line
        + if app.deleted_has_more {
            ui.spacing().interact_size.y + ui.spacing().item_spacing.y
        } else {
            0.0
        };
    let footer_rect = egui::Panel::bottom("deleted-list-footer")
        .resizable(false)
        .exact_size(footer_height)
        .show_separator_line(false)
        .frame(
            Frame::new()
                .fill(palette.sidebar())
                .inner_margin(Margin::same(12)),
        )
        .show(ui, |ui| ui.max_rect())
        .inner;
    Frame::new()
        .inner_margin(Margin::symmetric(14, 10))
        .show(ui, |ui| {
            let room = header_room(ui, 240.0);
            ui.add_space(room.top);
            ui.allocate_ui_with_layout(
                vec2(ui.available_width(), 38.0),
                Layout::left_to_right(Align::Center),
                |ui| {
                    ui.add_space(room.left);
                    if narrow
                        && theme::icon_button(
                            ui,
                            Icon::ArrowLeft,
                            18.0,
                            palette.secondary,
                            palette.text,
                            "Back to chats",
                        )
                        .clicked()
                    {
                        actions.push(Action::Open(Page::Chats));
                    }
                    let title_width = (ui.available_width() - 32.0 - room.right).max(0.0);
                    ui.allocate_ui_with_layout(
                        vec2(title_width, 28.0),
                        Layout::left_to_right(Align::Center),
                        |ui| {
                            theme::text(ui, "Deleted messages", theme::bold(20.0), palette.text);
                        },
                    );
                    ui.add_enabled_ui(!app.deleted_loading, |ui| {
                        if theme::icon_button(
                            ui,
                            Icon::Refresh,
                            17.0,
                            palette.secondary,
                            palette.text,
                            "Refresh saved messages",
                        )
                        .clicked()
                        {
                            actions.push(Action::RefreshDeleted);
                        }
                    });
                },
            );
            ui.add_space(5.0);
            let width = ui.available_width();
            widgets::search_field(
                ui,
                &palette,
                search_id(),
                &mut app.deleted_query,
                "Search saved messages",
                width,
            );
            if let Some(error) = &app.deleted_error {
                ui.add_space(8.0);
                ui.add(
                    egui::Label::new(
                        RichText::new(error)
                            .font(theme::regular(12.0))
                            .color(palette.danger),
                    )
                    .wrap(),
                );
            }
        });
    *groups = sender_groups(app);
    keep_visible_selection(groups, selected);
    let before_keyboard = selected.clone();
    search_keyboard(ui, groups, selected, show_thread, narrow);
    let reveal_selected = *selected != before_keyboard;
    let account = app.account().id.as_str();
    egui::ScrollArea::vertical()
        .id_salt(("deleted-senders-scroll", account))
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.spacing_mut().item_spacing.y = 0.0;
            if groups.is_empty() {
                if app.deleted_loading {
                    skeleton_rows(ui, &palette);
                } else if app.deleted_error.is_none() {
                    Frame::new().inner_margin(Margin::same(20)).show(ui, |ui| {
                        ui.add_space(12.0);
                        let (title, detail) = if !app.deleted_query.trim().is_empty() {
                            ("No matching messages", "Try a different search or load older messages.")
                        } else if app.deleted_has_more {
                            ("No deletions in this page", "Load more to check older saved messages.")
                        } else {
                            ("No saved deletions yet", "Originals appear here when a captured message is removed from its chat.")
                        };
                        theme::text(ui, title, theme::semibold(14.0), palette.text);
                        ui.add_space(6.0);
                        small(ui, &palette, detail);
                    });
                }
            }
            for group in groups.iter() {
                let latest = &app.deleted_messages[group.latest];
                let response = sender_row(ui, &palette, app.locale, account, group, latest, selected.as_deref() == Some(&group.key));
                if reveal_selected && selected.as_deref() == Some(&group.key) {
                    response.scroll_to_me(Some(Align::Center));
                }
                if response.clicked() {
                    *selected = Some(group.key.clone());
                    *show_thread = narrow;
                }
            }
        });
    let mut footer = ui.new_child(
        egui::UiBuilder::new()
            .id_salt("deleted-footer-controls")
            .max_rect(footer_rect)
            .layout(Layout::top_down(Align::Min)),
    );
    footer.set_clip_rect(footer_rect);
    if app.deleted_has_more
        && footer
            .add_enabled(!app.deleted_loading, egui::Button::new("Load more"))
            .clicked()
    {
        actions.push(Action::MoreDeleted);
    }
    small(
        &mut footer,
        &palette,
        if app.deleted_loading {
            "Loading saved messages…"
        } else {
            "Search covers loaded messages."
        },
    );
}

fn header_room(ui: &egui::Ui, content: f32) -> super::CaptionRoom {
    let mut room = super::caption_room(ui.ctx(), ui.max_rect(), content);
    if theme::macos_chrome(ui.ctx()) {
        let overhang = (theme::traffic_light_inset(ui.ctx()) - ui.max_rect().left()).max(0.0);
        if overhang > 0.0 {
            if content + overhang <= ui.available_width() {
                room.left = room.left.max(overhang);
            } else {
                // Narrow headers move below the traffic lights instead of clipping their title.
                room.top = room
                    .top
                    .max((theme::TOP_BAR_HEIGHT - ui.max_rect().top()).max(0.0));
            }
        }
    }
    room
}

fn search_keyboard(
    ui: &egui::Ui,
    groups: &[SenderGroup],
    selected: &mut Option<String>,
    show_thread: &mut bool,
    narrow: bool,
) {
    if groups.is_empty() || !ui.memory(|memory| memory.has_focus(search_id())) {
        return;
    }
    let (down, up, enter) = ui.input_mut(|input| {
        (
            super::keys::take_plain(input, egui::Key::ArrowDown),
            super::keys::take_plain(input, egui::Key::ArrowUp),
            super::keys::take_plain(input, egui::Key::Enter),
        )
    });
    let current = groups
        .iter()
        .position(|group| Some(&group.key) == selected.as_ref())
        .unwrap_or(0);
    if down || up {
        let next = if down {
            (current + 1).min(groups.len() - 1)
        } else {
            current.saturating_sub(1)
        };
        *selected = Some(groups[next].key.clone());
    }
    if enter {
        *show_thread = narrow;
        ui.memory_mut(|memory| memory.surrender_focus(search_id()));
    }
}

fn sender_row(
    ui: &mut egui::Ui,
    palette: &Palette,
    locale: Locale,
    account: &str,
    group: &SenderGroup,
    latest: &DeletedMessage,
    selected: bool,
) -> egui::Response {
    let (rect, _) = ui.allocate_exact_size(vec2(ui.available_width(), ROW_HEIGHT), Sense::hover());
    let response = ui
        .interact(rect, sender_row_id(account, &group.key), Sense::click())
        .on_hover_cursor(egui::CursorIcon::PointingHand);
    theme::reveal_focus(&response);
    let preview = preview_text(latest);
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::SelectableLabel,
            ui.is_enabled(),
            selected,
            format!("{}, {}, {}", group.name, message_count(group), preview),
        )
    });
    if ui.is_rect_visible(rect) {
        if selected || response.hovered() || response.has_focus() {
            ui.painter().rect_filled(
                rect.shrink2(vec2(6.0, 2.0)),
                6.0,
                if selected {
                    palette.surface_active
                } else {
                    palette.surface_hover
                },
            );
        }
        let avatar = Rect::from_center_size(rect.left_center() + vec2(32.0, 0.0), vec2(38.0, 38.0));
        widgets::paint_avatar(ui, palette, avatar, &group.name, &group.key, None);
        let left = rect.left() + 62.0;
        let stamp = crate::util::chat_stamp(locale, latest.saved.observed_at);
        let stamp_width = ui
            .painter()
            .layout_no_wrap(stamp.clone(), theme::regular(11.0), palette.secondary)
            .size()
            .x;
        let title = Rect::from_min_size(
            egui::pos2(left, rect.top() + 14.0),
            vec2((rect.right() - left - stamp_width - 28.0).max(0.0), 22.0),
        );
        let mut title_ui = ui.new_child(
            egui::UiBuilder::new()
                .id_salt(response.id.with("name"))
                .max_rect(title),
        );
        title_ui.set_clip_rect(title.intersect(ui.clip_rect()));
        theme::text(
            &mut title_ui,
            &group.name,
            theme::semibold(14.5),
            palette.text,
        );
        ui.painter().text(
            rect.right_top() + vec2(-14.0, 17.0),
            Align2::RIGHT_TOP,
            stamp,
            theme::regular(11.0),
            palette.secondary,
        );
        let count = group.rows.len().to_string();
        let preview_rect = Rect::from_min_size(
            egui::pos2(left, rect.top() + 38.0),
            vec2((rect.right() - left - 42.0).max(0.0), 20.0),
        );
        let mut preview_ui = ui.new_child(
            egui::UiBuilder::new()
                .id_salt(response.id.with("preview"))
                .max_rect(preview_rect),
        );
        preview_ui.set_clip_rect(preview_rect.intersect(ui.clip_rect()));
        theme::text(
            &mut preview_ui,
            preview,
            theme::regular(12.5),
            palette.secondary,
        );
        ui.painter().text(
            rect.right_top() + vec2(-16.0, 40.0),
            Align2::RIGHT_TOP,
            count,
            theme::medium(11.5),
            palette.secondary,
        );
    }
    theme::focus_outline(ui, response.id, rect.shrink(2.0), 6.0);
    response
}

fn preview_text(row: &DeletedMessage) -> String {
    if row.saved.text.trim().is_empty() {
        if row.saved.attachments.is_empty() {
            "Saved message".to_owned()
        } else {
            "Attachment".to_owned()
        }
    } else {
        row.saved
            .text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn message_count(group: &SenderGroup) -> String {
    let count = group.rows.len();
    let kind = if group.matching { "matching" } else { "saved" };
    let noun = if count == 1 { "message" } else { "messages" };
    format!("{count} {kind} {noun}")
}

fn thread(
    app: &App,
    ui: &mut egui::Ui,
    groups: &[SenderGroup],
    selected: Option<&str>,
    narrow: bool,
    actions: &mut Vec<Action>,
) {
    let palette = app.palette;
    let group = groups
        .iter()
        .find(|group| Some(group.key.as_str()) == selected);
    Frame::new()
        .fill(palette.chat)
        .inner_margin(Margin::ZERO)
        .show(ui, |ui| {
            ui.set_min_size(ui.available_size());
            let Some(group) = group else {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        theme::icon(ui, Icon::Archive, 36.0, palette.dim);
                        ui.add_space(12.0);
                        theme::text(ui, "Saved originals", theme::semibold(18.0), palette.text);
                        ui.add_space(5.0);
                        small(
                            ui,
                            &palette,
                            "Select a sender to read their retained messages.",
                        );
                    });
                });
                return;
            };
            let header_width = ui.available_width();
            let header = Frame::new()
                .fill(palette.panel)
                .inner_margin(Margin::symmetric(18, 8))
                .show(ui, |ui| {
                    ui.set_width((header_width - 36.0).max(0.0));
                    let room = header_room(ui, 200.0);
                    ui.add_space(room.top);
                    ui.allocate_ui_with_layout(
                        vec2(ui.available_width(), 44.0),
                        Layout::left_to_right(Align::Center),
                        |ui| {
                            ui.add_space(room.left);
                            if narrow
                                && theme::icon_button(
                                    ui,
                                    Icon::ArrowLeft,
                                    19.0,
                                    palette.secondary,
                                    palette.text,
                                    "Back to senders",
                                )
                                .clicked()
                            {
                                actions.push(Action::BackDeleted);
                            }
                            widgets::avatar(ui, &palette, &group.name, &group.key, 36.0, None);
                            ui.add_space(4.0);
                            let name_width = (ui.available_width() - room.right).max(0.0);
                            ui.allocate_ui_with_layout(
                                vec2(name_width, 38.0),
                                Layout::top_down(Align::Min),
                                |ui| {
                                    ui.set_width(name_width);
                                    theme::text(
                                        ui,
                                        &group.name,
                                        theme::semibold(16.0),
                                        palette.text,
                                    );
                                    theme::text(
                                        ui,
                                        format!("{} · independent copies", message_count(group)),
                                        theme::regular(12.0),
                                        palette.secondary,
                                    );
                                },
                            );
                        },
                    );
                });
            ui.painter().hline(
                header.response.rect.x_range(),
                header.response.rect.bottom(),
                egui::Stroke::new(1.0, palette.outline),
            );
            if app.capture_export.is_some() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Exporting plaintext…");
                    if ui.small_button("Cancel export").clicked() {
                        actions.push(Action::CancelCaptureExport);
                    }
                });
            }
            egui::ScrollArea::vertical()
                .id_salt(("deleted-thread", app.account().id.as_str(), &group.key))
                .auto_shrink([false, false])
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    Frame::new()
                        .inner_margin(Margin::symmetric(if narrow { 14 } else { 28 }, 16))
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            let mut previous_day = None;
                            let mut previous_chat: Option<&str> = None;
                            for &index in &group.rows {
                                let row = &app.deleted_messages[index];
                                let day = crate::util::day_key(row.saved.observed_at);
                                if day != previous_day || previous_chat.is_none() {
                                    day_separator(ui, &palette, app.locale, row.saved.observed_at);
                                    previous_day = day;
                                }
                                if previous_chat != Some(&row.canonical_chat) {
                                    ui.add_space(7.0);
                                    ui.horizontal(|ui| {
                                        theme::icon(ui, Icon::MessageCircle, 12.0, palette.dim);
                                        theme::text(
                                            ui,
                                            &row.chat_name,
                                            theme::medium(11.5),
                                            palette.secondary,
                                        );
                                    });
                                    ui.add_space(3.0);
                                    previous_chat = Some(&row.canonical_chat);
                                }
                                ui.push_id((row.saved.sequence, "retained-message"), |ui| {
                                    message_bubble(
                                        ui,
                                        &palette,
                                        app.locale,
                                        row,
                                        app.capture_export.is_some(),
                                        actions,
                                    );
                                });
                                ui.add_space(7.0);
                            }
                            ui.add_space(8.0);
                        });
                });
        });
}

fn day_separator(ui: &mut egui::Ui, palette: &Palette, locale: Locale, timestamp: i64) {
    ui.add_space(8.0);
    ui.vertical_centered(|ui| {
        widgets::chip(ui, palette, &crate::util::day_label(locale, timestamp));
    });
    ui.add_space(10.0);
}

fn message_bubble(
    ui: &mut egui::Ui,
    palette: &Palette,
    locale: Locale,
    row: &DeletedMessage,
    exporting: bool,
    actions: &mut Vec<Action>,
) {
    let available = ui.available_width();
    let max_width = available.clamp(0.0, 610.0);
    let text_width = ui
        .painter()
        .layout_no_wrap(row.saved.text.clone(), theme::regular(14.0), palette.text)
        .size()
        .x;
    let width = (text_width + 24.0).max(230.0).min(max_width);
    let outgoing = row.saved.outgoing;
    ui.with_layout(
        Layout::top_down(if outgoing { Align::RIGHT } else { Align::LEFT }),
        |ui| {
            Frame::new()
                .fill(if outgoing {
                    palette.bubble_out
                } else {
                    palette.bubble_in
                })
                .corner_radius(8)
                .inner_margin(Margin::symmetric(11, 8))
                .show(ui, |ui| {
                    ui.spacing_mut().item_spacing.y = 2.0;
                    ui.spacing_mut().interact_size.y = 18.0;
                    ui.spacing_mut().button_padding.y = 2.0;
                    ui.set_width((width - 22.0).max(0.0));
                    if !row.saved.text.is_empty() {
                        body(ui, palette, &row.saved.text);
                    }
                    if !row.saved.attachments.is_empty() {
                        ui.horizontal_wrapped(|ui| {
                            theme::icon(ui, Icon::Paperclip, 13.0, palette.secondary);
                            small(ui, palette, &attachment_summary(&row.saved.attachments));
                        });
                    }
                    if row.saved.text.is_empty() && row.saved.attachments.is_empty() {
                        small(ui, palette, "Saved message");
                    }
                    ui.add_space(4.0);
                    ui.horizontal_wrapped(|ui| {
                        small(ui, palette, &row.reason);
                        small(
                            ui,
                            palette,
                            &format!("· Saved {}", crate::util::clock(row.saved.observed_at)),
                        );
                    });
                    let revisions = row
                        .saved
                        .history
                        .iter()
                        .filter(|event| matches!(event.kind.as_str(), "edited" | "edit_accepted"))
                        .count();
                    let details = match revisions {
                        0 => "Details".to_owned(),
                        1 => "Details · 1 revision".to_owned(),
                        _ => format!("Details · {revisions} revisions"),
                    };
                    egui::CollapsingHeader::new(
                        RichText::new(details)
                            .font(theme::regular(11.5))
                            .color(palette.secondary),
                    )
                    .id_salt("retained-details")
                    .show(ui, |ui| {
                        small(
                            ui,
                            palette,
                            &format!(
                                "Saved {}",
                                crate::util::moment_stamp(locale, row.saved.observed_at)
                            ),
                        );
                        if row.saved.sent_at != row.saved.observed_at {
                            small(
                                ui,
                                palette,
                                &format!(
                                    "Sent {}",
                                    crate::util::moment_stamp(locale, row.saved.sent_at)
                                ),
                            );
                        }
                        attachments(
                            ui,
                            palette,
                            &row.canonical_chat,
                            &row.saved.attachments,
                            exporting,
                            actions,
                        );
                        for observation in &row.saved.history {
                            ui.add_space(9.0);
                            observation_row(
                                ui,
                                palette,
                                locale,
                                &row.canonical_chat,
                                observation,
                                exporting,
                                actions,
                            );
                        }
                        if row.saved.history_truncated {
                            small(
                                ui,
                                palette,
                                "Export the transcript to include the complete history.",
                            );
                        }
                        ui.add_space(8.0);
                        small(
                            ui,
                            palette,
                            "Exports are plaintext files in a folder you choose.",
                        );
                        if ui
                            .add_enabled(!exporting, egui::Button::new("Export transcript").small())
                            .clicked()
                        {
                            actions.push(Action::ExportCapture {
                                chat: Some(row.canonical_chat.clone()),
                            });
                        }
                    });
                });
        },
    );
}

fn attachment_summary(attachments: &[SavedAttachment]) -> String {
    let saved = attachments
        .iter()
        .filter(|attachment| attachment.path.is_some())
        .count();
    let noun = if attachments.len() == 1 {
        "attachment"
    } else {
        "attachments"
    };
    if saved == attachments.len() {
        format!("{} {noun} saved", attachments.len())
    } else {
        format!("{} {noun} · {saved} saved", attachments.len())
    }
}

fn observation_row(
    ui: &mut egui::Ui,
    palette: &Palette,
    locale: Locale,
    chat: &str,
    observation: &SavedObservation,
    exporting: bool,
    actions: &mut Vec<Action>,
) {
    small(
        ui,
        palette,
        &format!(
            "{} · {}",
            history_label(&observation.kind),
            crate::util::moment_stamp(locale, observation.observed_at)
        ),
    );
    if !observation.text.is_empty() {
        body(ui, palette, &observation.text);
    }
    attachments(
        ui,
        palette,
        chat,
        &observation.attachments,
        exporting,
        actions,
    );
}

fn attachments(
    ui: &mut egui::Ui,
    palette: &Palette,
    chat: &str,
    attachments: &[SavedAttachment],
    exporting: bool,
    actions: &mut Vec<Action>,
) {
    for (index, attachment) in attachments.iter().enumerate() {
        ui.add_space(5.0);
        if let Some(path) = &attachment.path {
            let label = if attachments.len() > 1 {
                format!("Save attachment {}", index + 1)
            } else {
                "Save attachment".to_owned()
            };
            if ui
                .add_enabled(!exporting, egui::Button::new(label).small())
                .clicked()
            {
                actions.push(Action::ExportSavedAttachment {
                    chat: chat.to_owned(),
                    path: path.clone(),
                });
            }
        } else {
            let status = match attachment.status.as_str() {
                "queued" => "Attachment waiting to download",
                "downloading" => "Attachment downloading",
                "paused" => "Attachment download paused",
                "failed" => "Attachment unavailable",
                "oversized" => "Attachment exceeds the archive size limit",
                _ => "Attachment has no saved file",
            };
            small(ui, palette, status);
        }
        if let Some(error) = &attachment.error {
            small(ui, palette, error);
        }
    }
}

fn skeleton_rows(ui: &mut egui::Ui, palette: &Palette) {
    for _ in 0..3 {
        let (rect, _) =
            ui.allocate_exact_size(vec2(ui.available_width(), ROW_HEIGHT), Sense::hover());
        ui.painter()
            .circle_filled(rect.left_center() + vec2(32.0, 0.0), 19.0, palette.surface);
        for (offset, width) in [(18.0_f32, 120.0_f32), (40.0, 160.0)] {
            let line = Rect::from_min_size(
                rect.left_top() + vec2(62.0, offset),
                vec2(width.min((rect.width() - 78.0).max(0.0)), 9.0),
            );
            ui.painter().rect_filled(line, 4.0, palette.surface);
        }
    }
}

fn body(ui: &mut egui::Ui, palette: &Palette, text: &str) {
    ui.add(
        egui::Label::new(
            RichText::new(text)
                .font(theme::regular(14.0))
                .color(palette.text),
        )
        .wrap()
        .selectable(true),
    );
}

fn small(ui: &mut egui::Ui, palette: &Palette, text: &str) {
    ui.add(
        egui::Label::new(
            RichText::new(text)
                .font(theme::regular(11.5))
                .color(palette.secondary),
        )
        .wrap(),
    );
}

fn matches_query(row: &DeletedMessage, query: &str) -> bool {
    contains_query(
        &[
            &row.chat_name,
            sender_name(row),
            &row.saved.text,
            &row.reason,
        ],
        query,
    )
}

fn contains_query(fields: &[&str], query: &str) -> bool {
    let searchable = fields.join("\n").to_lowercase();
    query
        .split_whitespace()
        .all(|word| searchable.contains(word))
}

fn history_label(kind: &str) -> &str {
    match kind {
        "message" | "outgoing_pending" => "Original saved",
        "edited" => "Edit received",
        "deleted" => "Deletion received",
        "edit_requested" => "Edit requested",
        "edit_accepted" => "Edit accepted",
        "edit_uncertain" => "Edit outcome uncertain",
        "revoke_requested" => "Deletion requested",
        "revoke_accepted" => "Deletion accepted",
        "revoke_uncertain" => "Deletion outcome uncertain",
        "send_accepted" => "Send accepted",
        "send_uncertain" => "Send outcome uncertain",
        _ => "Saved observation",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loaded_original_filter_matches_words_across_fields_and_unicode() {
        let fields = ["Book club", "Élodie", "The old message"];
        assert!(contains_query(&fields, ""));
        assert!(contains_query(&fields, "élodie old"));
        assert!(contains_query(&fields, "club message"));
        assert!(!contains_query(&fields, "club missing"));
    }

    #[test]
    fn sender_identity_groups_own_messages_and_separates_unknown_people() {
        assert_eq!(
            sender_key("phone@s.whatsapp.net", true, "one@g.us", "one"),
            sender_key("lid@lid", true, "two@g.us", "two")
        );
        assert_ne!(
            sender_key("", false, "one@g.us", "one"),
            sender_key("", false, "one@g.us", "two")
        );
        assert_eq!(
            sender_key("canonical@s.whatsapp.net", false, "one@g.us", "one"),
            sender_key("canonical@s.whatsapp.net", false, "two@g.us", "two")
        );
    }
}
