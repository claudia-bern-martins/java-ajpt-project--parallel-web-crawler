package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();
  private final ZonedDateTime startTime;

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    this.startTime = ZonedDateTime.now(clock);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T wrap(Class<T> klass, T delegate) {
    Objects.requireNonNull(klass);

    List<Method> profiledMethods =
            Arrays.stream(klass.getDeclaredMethods())
                    .filter(m -> m.isAnnotationPresent(Profiled.class))
                    .toList();
    
    if(profiledMethods.isEmpty()) {
        throw new IllegalArgumentException("Wrapped interface does not " +
                "contain any @Profiled methods");
    }

    return (T) Proxy.newProxyInstance(ProfilerImpl.class.getClassLoader(),
            new Class<?>[]{klass}, new ProfilingMethodInterceptor(clock, delegate, state));
  }

  @Override
  public void writeData(Path path) {
      Objects.requireNonNull(path);
      try(Writer writer = Files.newBufferedWriter(path)) {
          writeData(writer);
          writer.flush();
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
  }

  @Override
  public void writeData(Writer writer) throws IOException {
    writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
    writer.write(System.lineSeparator());
    state.write(writer);
    writer.write(System.lineSeparator());
  }
}
