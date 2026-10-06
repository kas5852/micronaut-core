/*
 * Copyright 2017-2026 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.http.client.sse;

import io.micronaut.core.annotation.Internal;
import io.micronaut.core.io.buffer.ReadBuffer;
import io.micronaut.core.type.Argument;
import io.micronaut.core.type.Headers;
import io.micronaut.http.ByteBodyHttpResponse;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.body.BodyElements;
import io.micronaut.http.body.CloseableByteBody;
import io.micronaut.http.body.MessageBodyHandlerRegistry;
import io.micronaut.http.body.MessageBodyReader;
import io.micronaut.http.body.PieceReader;
import io.micronaut.http.body.stream.ByteBodyElements;
import io.micronaut.http.client.ElementsResponse;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.micronaut.http.sse.Event;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.Function;

/**
 * The events of the response of an {@link AsyncSseClient} exchange, decoded from the body bytes
 * the client received.
 *
 * @author Denis Stepanov
 * @since 5.3.0
 */
@Internal
public final class EventStreams {

    private EventStreams() {
    }

    /**
     * The response of an exchange whose body bytes the client received: the events of an event
     * stream, decoded as they are read, or a body of another type, decoded whole as one event.
     * The events take over the bytes of the response.
     *
     * @param response        The response, with a status that is not an error
     * @param handlerRegistry The readers of the event data
     * @param eventType       The event data type
     * @param maxBufferSize   The maximum size of a line, and of the data of one event
     * @param <B>             The event data type
     * @return The response, whose body is the events
     */
    public static <B> HttpResponse<BodyElements<Event<B>>> response(ByteBodyHttpResponse<?> response,
                                                                   MessageBodyHandlerRegistry handlerRegistry,
                                                                   Argument<B> eventType,
                                                                   long maxBufferSize) {
        CloseableByteBody body = response.byteBody().move();
        try {
            MediaType contentType = response.getContentType().orElse(null);
            HttpHeaders headers = response.getHeaders();
            BodyElements<Event<B>> elements;
            if (contentType != null && MediaType.TEXT_EVENT_STREAM_TYPE.matches(contentType)) {
                // the data of each event is JSON
                elements = new ByteBodyElements<>(body, reader(handlerRegistry, eventType, headers, maxBufferSize), EventStreams::wrap);
            } else {
                // a single body, such as JSON, is one event
                MediaType mediaType = contentType == null ? MediaType.APPLICATION_JSON_TYPE : contentType;
                elements = new SingleBodyElements<>(body, dataReader(handlerRegistry, eventType, mediaType, headers));
            }
            return ElementsResponse.of(response, elements);
        } catch (RuntimeException e) {
            body.close();
            throw e;
        }
    }

    /**
     * The reader of the events of an event stream: the lines are split as the pieces are read,
     * and the data of an event is decoded as JSON when the event is polled.
     *
     * @param handlerRegistry The readers of the event data
     * @param eventType       The event data type
     * @param headers         The headers of the response
     * @param maxBufferSize   The maximum size of a line, and of the data of one event
     * @param <B>             The event data type
     * @return The reader
     */
    public static <B> PieceReader<Event<B>> reader(MessageBodyHandlerRegistry handlerRegistry,
                                                   Argument<B> eventType,
                                                   Headers headers,
                                                   long maxBufferSize) {
        return new EventReader<>(new EventStreamDecoder(maxBufferSize), dataReader(handlerRegistry, eventType, MediaType.APPLICATION_JSON_TYPE, headers));
    }

    /**
     * The failure of the events, an {@link HttpClientException}.
     *
     * @param error A failure to read or decode the events
     * @return The failure of the events
     */
    static Throwable wrap(Throwable error) {
        return error instanceof HttpClientException ? error : new HttpClientException("Error consuming Server Sent Events: " + error.getMessage(), error);
    }

    private static <B> Function<byte[], B> dataReader(MessageBodyHandlerRegistry handlerRegistry,
                                                      Argument<B> eventType,
                                                      MediaType mediaType,
                                                      Headers headers) {
        MessageBodyReader<B> reader = handlerRegistry.getReader(eventType, List.of(mediaType));
        return data -> {
            // a stream over the array: a buffer of it would be copied again to be decoded
            B decoded = reader.read(eventType, mediaType, headers, new ByteArrayInputStream(data));
            if (decoded == null) {
                throw new HttpClientException("Event data decoded to null for type " + eventType);
            }
            return decoded;
        };
    }

    /**
     * Reads the events of the pieces of an event stream. The lines are split as the pieces are
     * read, and the data of an event is decoded when the event is polled.
     *
     * @param <B> The event data type
     */
    private static final class EventReader<B> implements PieceReader<Event<B>> {
        private final EventStreamDecoder decoder;
        private final Function<byte[], B> dataReader;
        private final ArrayDeque<Event<byte[]>> events = new ArrayDeque<>(1);
        /**
         * The bytes of a piece that is not a heap buffer.
         */
        private byte[] scratch = new byte[0];

        EventReader(EventStreamDecoder decoder, Function<byte[], B> dataReader) {
            this.decoder = decoder;
            this.dataReader = dataReader;
        }

        @Override
        public void read(ReadBuffer piece) {
            try (piece) {
                // a heap buffer is decoded in place, another one is copied into an array that is reused
                int length = piece.readable();
                List<Event<byte[]>> decoded = piece.useFastHeapBuffer(nio -> decoder.decode(nio.array(), nio.arrayOffset() + nio.position(), nio.remaining()));
                if (decoded == null) {
                    if (scratch.length < length) {
                        scratch = new byte[Math.max(length, scratch.length * 2)];
                    }
                    piece.toArray(scratch, 0);
                    decoded = decoder.decode(scratch, 0, length);
                }
                events.addAll(decoded);
            }
        }

        @Override
        public void complete() {
            // an event not terminated by a blank line is discarded
        }

        @Override
        public @Nullable Event<B> poll() {
            Event<byte[]> event = events.poll();
            return event == null ? null : Event.of(event, dataReader.apply(event.getData()));
        }

        @Override
        public void close() {
            events.clear();
        }
    }
}
