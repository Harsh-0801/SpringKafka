package com.example.products.service;

import com.example.products.model.CreateProductRestModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ProductServiceImpl implements ProductService{
    private static final Logger log = LoggerFactory.getLogger(ProductServiceImpl.class);

    private final KafkaTemplate<String ,ProductCreatedEvent> kafkaTemplate;

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Override
    public String createProduct(CreateProductRestModel productRestModel) {

        String productId = UUID.randomUUID().toString();

        //TODO: Store the product into the database before publishing event

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId, productRestModel.getTitle(), productRestModel.getPrice(),
                productRestModel.getQuantity());
        try {
            kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send kafka message for product id: {}", productId, ex);
                        } else {
                            log.info("Message sent successfully for productId: {} | Topic: {} | Partition: {} | Offset: {}",
                                    productId,
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());

                        }

                    });
        }
        catch (Exception e){
            log.error("Error while sending Kafka message for productId: {}", productId, e);
            throw new RuntimeException("Error sending Kafka message", e);
        }

            return productId;
        }

}
