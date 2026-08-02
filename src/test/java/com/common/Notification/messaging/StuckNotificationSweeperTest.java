package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationDispatchView;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.NotificationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StuckNotificationSweeperTest {

    @Mock
    private NotificationRepository notificationRepository;
    @Mock
    private NotificationDispatcher dispatcher;

    private StuckNotificationSweeper sweeper;

    @BeforeEach
    void setUp() {
        sweeper = new StuckNotificationSweeper(notificationRepository, dispatcher);
    }

    private NotificationDispatchView view(String id, Channel channel) {
        NotificationDispatchView view = mock(NotificationDispatchView.class);
        when(view.getId()).thenReturn(id);
        when(view.getChannel()).thenReturn(channel);
        return view;
    }

    private void stubSweep(List<NotificationDispatchView> results) {
        when(notificationRepository.findTop200ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                any(NotificationStatus.class), any(Instant.class))).thenReturn(results);
    }

    @Test
    @DisplayName("every stuck notification is republished on its own channel")
    void republishesStuckNotifications() {
        stubSweep(List.of(view("n-1", Channel.EMAIL), view("n-2", Channel.SMS)));

        sweeper.republishStuckNotifications();

        verify(dispatcher).dispatch("n-1", Channel.EMAIL);
        verify(dispatcher).dispatch("n-2", Channel.SMS);
    }

    @Test
    @DisplayName("only ACCEPTED rows are swept — QUEUED ones are already on the topic")
    void sweepsOnlyAcceptedRows() {
        stubSweep(List.of());

        sweeper.republishStuckNotifications();

        ArgumentCaptor<NotificationStatus> status = ArgumentCaptor.forClass(NotificationStatus.class);
        verify(notificationRepository).findTop200ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                status.capture(), any(Instant.class));
        assertThat(status.getValue()).isEqualTo(NotificationStatus.ACCEPTED);
    }

    @Test
    @DisplayName("a quiet sweep does nothing at all")
    void doesNothingWhenNoneStuck() {
        stubSweep(List.of());

        sweeper.republishStuckNotifications();

        verifyNoInteractions(dispatcher);
    }

    @Test
    @DisplayName("the sweep reads a projection, never full entities")
    void usesProjectionNotEntities() {
        stubSweep(List.of(view("n-1", Channel.EMAIL)));

        sweeper.republishStuckNotifications();

        // Regression guard: this used to load NotificationRecord entities, dragging every @Lob
        // body and recipient into memory to read two fields — worst during the outage that
        // creates the backlog in the first place.
        verify(notificationRepository, never()).findAll();
        verify(dispatcher).dispatch(eq("n-1"), eq(Channel.EMAIL));
    }
}