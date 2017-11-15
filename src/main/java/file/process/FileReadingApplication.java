package file.process;

import java.io.File;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;

import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;

@SpringBootApplication
public class FileReadingApplication {

	private static final String INBOUND_PATH = "/file-path-to-process";
	private static final String PROCESS_INTERVAL_MILLISECONDS = "5000";

	public static void main(String[] args) {
		new SpringApplicationBuilder(FileReadingApplication.class).web(false).run(args);
	}

	@Bean
	public MessageChannel fileInputChannel() {
		return new DirectChannel();
	}

	@Bean
	public MessageChannel processFileChannel() {
		DirectChannel dc = new DirectChannel();
		dc.subscribe(processFileOutbound());
		return dc;

	}

	@ServiceActivator(inputChannel = "processFileChannel", autoStartup = "true")
	private MessageHandler processFileOutbound() {
		return new Handler();
	}

	@Bean
	@InboundChannelAdapter(value = "fileInputChannel", poller = @Poller(fixedDelay = PROCESS_INTERVAL_MILLISECONDS))
	public MessageSource<File> fileReadingMessageSource() {
		FileReadingMessageSource source = new FileReadingMessageSource();
		source.setDirectory(new File(INBOUND_PATH));
		//source.setFilter(new SimplePatternFileListFilter("*.txt"));
		
		CompositeFileListFilter<File> compositeFileFilter = new CompositeFileListFilter<File>();
		compositeFileFilter.addFilter(new SimplePatternFileListFilter("*.txt"));
		compositeFileFilter.addFilter(new AcceptOnceFileListFilter<File>());
		source.setFilter(compositeFileFilter);
		return source;
	}

	@Bean
	@Transformer(inputChannel = "fileInputChannel", outputChannel = "processFileChannel")
	public FileToStringTransformer fileToStringTransformer() {
		return new FileToStringTransformer();
	}

	public class Handler implements MessageHandler {

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			System.out.println(message);

		}

	}

}