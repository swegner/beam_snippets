package swegner.beam.snippets;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Example demonstrating the Injected Factory PipelineOptions pattern.
 * This is used in various places in the Beam Java SDK, for example in
 * GcpOptions getCredentials():
 *   https://www.google.com/url?q=https://github.com/apache/beam/blob/5d3c6453dfe867eeb9268ef7113d4f5fa6090ee3/sdks/java/extensions/google-cloud-platform-core/src/main/java/org/apache/beam/sdk/extensions/gcp/options/GcpOptions.java%23L125&sa=D&usg=AFQjCNGpbUAG2B-FjGpvj57epguslipTzg
 */
@RunWith(JUnit4.class)
public class InjectedFactoryPipelineOptionsPattern {
  /** The interface for the service dependency to inject into a pipeline. */
  public interface MyService {
    /** The API of the service dependency to inject. */
    String getResource(int id);
  }

  /** Factory interface for creating instances of MyService. */
  public interface MyServiceFactory {
    MyService create();
  }

  public interface MyOptions extends PipelineOptions {
    /**
     * Pipeline components can retrieve the service dependency via getMyService().
     * Because it is marked @JsonIgnore, the value will be retrieved via the
     * MyServiceFactoryInvoker using the factory provided in getMyServiceFactoryClass().
     */
    @JsonIgnore
    @Default.InstanceFactory(MyServiceFactoryInvoker.class)
    MyService getMyService();
    void setMyService(MyService service);

    /**
     * Factory for producing the actual service instance. The @Default.Class(..) specifies
     * the actual factory, while tests can set their factory for injecting test data.
     */
    @Default.Class(RealMyServiceFactory.class)
    Class<? extends MyServiceFactory> getMyServiceFactoryClass();
    void setMyServiceFactoryClass(Class<? extends MyServiceFactory> factoryClass);
  }

  /**
   * Factory invoker simply instantiates the factory class via reflection and invokes
   * it to generate the service dependency instance.
   */
  public static class MyServiceFactoryInvoker implements DefaultValueFactory<MyService> {
    @Override
    public MyService create(PipelineOptions options) {
      MyOptions myOptions = options.as(MyOptions.class);
      MyServiceFactory factory = InstanceBuilder
          .ofType(MyServiceFactory.class)
          .fromClass(myOptions.getMyServiceFactoryClass())
          .build();

      return factory.create();
    }
  }

  /** Actual implementation of service factory. */
  public static class RealMyServiceFactory implements MyServiceFactory {
    /* Return actual implementation of MyService. */
    @Override
    public MyService create() {
      return id -> String.format("foobar-%d", id);
    }
  }

  /** DoFn which utilizes MyService dependency. */
  public static class GetResource extends DoFn<Integer, String> {
    private transient MyService myService;

    /** Retrieve the service instance during StartBundle from PipelineOptions. */
    @StartBundle
    public void startBundle(StartBundleContext context) {
      myService = context.getPipelineOptions().as(MyOptions.class).getMyService();
    }

    @ProcessElement
    public void processElement(@Element Integer id, OutputReceiver<String> receiver) {
      receiver.output(myService.getResource(id));
    }
  }

  @Test
  public void testGetResource() {
    MyOptions options = PipelineOptionsFactory.as(MyOptions.class);
    // Inject fake MyServiceFactory for testing.
    options.setMyServiceFactoryClass(TestMyServiceFactory.class);
    Pipeline pipeline = TestPipeline.create(options);

    PCollection output = pipeline
        .apply(Create.of(1234))
        .apply(ParDo.of(new GetResource()));

    PAssert.<String>thatSingleton(output).isEqualTo("test-1234");
    pipeline.run();
  }

  public static class TestMyServiceFactory implements MyServiceFactory {
    /** Return test fake implementation of MyService. */
    @Override
    public MyService create() {
      return id -> String.format("test-%d", id);
    }
  }
}
