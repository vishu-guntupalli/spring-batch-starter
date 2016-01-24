package com.vishu.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.vishu.batch.model.DiscountProduct;
import com.vishu.batch.model.Product;

public class ProductItemProcessor implements ItemProcessor<Product, DiscountProduct>{

	public DiscountProduct process(Product item) throws Exception {
        DiscountProduct discountProduct = new DiscountProduct(item);
		if( !((item.getProductId() % 2) == 1)) {
        	discountProduct.setDiscountAvailable(true);
        }
		return discountProduct;
	}

}
