package io.gingersnapproject.hotrod;

import static java.lang.String.format;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.ProtocolFlag;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.concurrent.TimeoutException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.gingersnapproject.Caches;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.smallrye.mutiny.Uni;

public record CommandProcessor(Channel channel, Caches maps) {
   private static final Log log = LogFactory.getLog(CommandProcessor.class, Log.class);

   private static String toString(byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
   }

   private static byte[] toByteArray(String str) {
      return str == null ? null : str.getBytes(StandardCharsets.UTF_8);
   }

   private Cache<String, List<byte[]>> getOrCreateMultimap(String name) {
      return maps.getMultiMaps().computeIfAbsent(name, ___ -> Caffeine.newBuilder().build());
   }

   public void put(GingersnapHeader header, byte[] key, byte[] value) {
      if (header.hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         writeException(header, new UnsupportedOperationException("previous value is not supported!"));
         return;
      }

      try {
         maps.put(header.getCacheName(), toString(key), toString(value));
         writeSuccess(header);
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   private void handleGetResponse(GingersnapHeader header, byte[] value) {
      if (value == null) {
         writeNotExist(header);
      } else {
         writeResponse(header.encoder().valueResponse(header, null, channel, OperationStatus.Success, value));
      }
   }

   public void get(GingersnapHeader header, byte[] key) {
      try {
         // This can never be null
         Uni<String> uniValue = maps.get(header.getCacheName(), toString(key));
         uniValue.subscribe().with(s -> handleGetResponse(header, toByteArray(s)),
               t -> writeException(header, t));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   public void remove(GingersnapHeader header, byte[] key) {
      if (header.hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         writeException(header, new UnsupportedOperationException("previous value is not supported!"));
         return;
      }

      try {
         if (maps.remove(header.getCacheName(), toString(key))) {
            this.writeSuccess(header);
         } else {
            this.writeNotExist(header);
         }
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   public void ping(GingersnapHeader header) {
      this.writeResponse(header.encoder().pingResponse(header, null, this.channel, OperationStatus.Success));
   }

   public void putAll(GingersnapHeader header, Map<byte[], byte[]> entryMap) {

      try {
         String cacheName = header.getCacheName();
         for (Map.Entry<byte[], byte[]> entry : entryMap.entrySet()) {
            maps.put(cacheName, toString(entry.getKey()), toString(entry.getValue()));
         }
         writeSuccess(header);
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   public void stats(GingersnapHeader header) {
      writeException(header, new UnsupportedOperationException("Stats not supported yet!"));
   }

   public void query(GingersnapHeader header, byte[] query) {
      writeException(header, new UnsupportedOperationException("Query not supported yet!"));
   }

   public void exec(GingersnapHeader header, String task, Map<String, byte[]> taskParams) {
      // TODO: do we support tasks? If anything this is just so adding a new cache doesn't error
      writeResponse(header.encoder().valueResponse(header, null, channel, OperationStatus.Success, Util.EMPTY_BYTE_ARRAY));
   }

   public void iterationStart(GingersnapHeader header, byte[] segmentMask, String filterConverterFactory, List<byte[]> filterConverterParams, int batchSize, boolean includeMetadat) {
      writeException(header, new UnsupportedOperationException("Iteration not supported yet!"));
   }

   public void iterationNext(GingersnapHeader header, String iterationId) {
      writeException(header, new UnsupportedOperationException("Iteration not supported yet!"));
   }

   public void iterationEnd(GingersnapHeader header, String iterationId) {
      writeException(header, new UnsupportedOperationException("Iteration not supported yet!"));
   }

   void writeResponse(ByteBuf buf) {
      channel.writeAndFlush(buf);
   }

   void writeSuccess(GingersnapHeader header) {
      writeResponse(header.encoder().emptyResponse(header, null, channel, OperationStatus.Success));
   }

   void writeNotExist(GingersnapHeader header) {
      writeResponse(header.encoder().notExistResponse(header, null, channel));
   }

   public void writeException(GingersnapHeader header, Throwable cause) {
      if (cause instanceof CompletionException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      String msg = cause.toString();
      OperationStatus status;
      if (cause instanceof InvalidMagicIdException) {
         log.exceptionReported(cause);
         status = OperationStatus.InvalidMagicOrMsgId;
      } else if (cause instanceof HotRodUnknownOperationException hruoe) {
         log.exceptionReported(cause);
         header = hruoe.toHeader();
         status = OperationStatus.UnknownOperation;
      } else if (cause instanceof UnknownVersionException uve) {
         log.exceptionReported(cause);
         header = uve.toHeader();
         status = OperationStatus.UnknownVersion;
      } else if (cause instanceof RequestParsingException rpe) {
         if (cause instanceof CacheNotFoundException)
            log.debug(cause.getMessage());
         else
            log.exceptionReported(cause);

         msg = cause.getCause() == null ? cause.toString() : format("%s: %s", cause.getMessage(), cause.getCause().toString());
         header = rpe.toHeader();
         status = OperationStatus.ParseError;
      } else if (cause instanceof IOException) {
         status = OperationStatus.ParseError;
      } else if (cause instanceof TimeoutException) {
         status = OperationStatus.OperationTimedOut;
      } else if (cause instanceof IllegalStateException) {
         // Some internal server code could throw this, so make sure it's logged
         log.exceptionReported(cause);
         if (header != null) {
            status = header.encoder().errorStatus(cause);
            msg = createErrorMsg(cause);
         } else {
            status = OperationStatus.ServerError;
         }
      } else if (header != null) {
         log.exceptionReported(cause);
         status = header.encoder().errorStatus(cause);
         msg = createErrorMsg(cause);
      } else {
         log.exceptionReported(cause);
         status = OperationStatus.ServerError;
      }
      if (header == null) {
         header = new GingersnapHeader(HotRodOperation.ERROR, (byte) 0, 0, "", 0, (short) 1, 0,
               MediaType.MATCH_ALL, MediaType.MATCH_ALL, null);
      } else {
         header = new GingersnapHeader(HotRodOperation.ERROR, header.getVersion(), header.getMessageId(), header.getCacheName(),
               header.getFlag(), header.getClientIntel(), header.getTopologyId(), header.getKeyMediaType(),
               header.getValueMediaType(), null);
      }
      ByteBuf buf = header.encoder().errorResponse(header, null, channel, msg, status);
      channel.writeAndFlush(buf);
   }

   private String createErrorMsg(Throwable t) {
      Set<Throwable> causes = new LinkedHashSet<>();
      Throwable initial = t;
      while (initial != null && !causes.contains(initial)) {
         causes.add(initial);
         initial = initial.getCause();
      }
      return causes.stream().map(Object::toString).collect(Collectors.joining("\n"));
   }

   public void getMultiMap(GingersnapHeader header, byte[] key) {
      Cache<String, List<byte[]>> map = getOrCreateMultimap(header.getCacheName());
      try {
         List<byte[]> list = map.getIfPresent(new String(key));
         if (list == null) {
            writeNotExist(header);
         } else {
            writeResponse(header.encoder().multimapCollectionResponse(header, null, channel, OperationStatus.Success, list));
         }
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   public void putMultiMap(GingersnapHeader header, byte[] key, byte[] value) {
      Cache<String, List<byte[]>> map = getOrCreateMultimap(header.getCacheName());
      try {
         map.asMap().compute(new String(key), (k, v) -> {
            if (v == null) {
               var list = new ArrayList<byte[]>();
               list.add(value);
               return list;
            }
            v.add(value);
            return v;
         });
         writeSuccess(header);
      } catch (Throwable t) {
         writeException(header, t);
      }
   }
}
