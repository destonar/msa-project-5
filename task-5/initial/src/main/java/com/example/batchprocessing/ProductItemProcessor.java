package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class ProductItemProcessor implements ItemProcessor<Product, Product> {

	private static final Logger log = LoggerFactory.getLogger(ProductItemProcessor.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

    final private String Cmd =
            """
            SELECT
                productsku AS "productSku",
                loyalitydata AS "loyalityData"
            FROM loyality_data
            WHERE productsku = ?
            """;
    @Override
	public Product process(final Product product) {
        var sku = product.productSku();
        var loyalties = jdbcTemplate.query(
                Cmd,
                new DataClassRowMapper<>(Loyality.class),
                sku
        );

        var loyaltyData = loyalties.isEmpty() ? null : loyalties.get(0).loyalityData();
        log.info("Product {} was processed", sku);

        return new Product(product.productId(), sku, product.productName(), product.productAmount(), loyaltyData);
	}

}
