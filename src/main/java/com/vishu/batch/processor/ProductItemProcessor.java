package com.vishu.batch.processor;

import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;

import com.vishu.batch.model.DiscountProduct;
import com.vishu.batch.model.Product;

public class ProductItemProcessor implements ItemProcessor<Product, DiscountProduct>{

	public DiscountProduct process(Product item) throws Exception {
        DiscountProduct discountProduct = new DiscountProduct(item);
        Predicate<Long> isEven = (x) -> (x%2 == 0);
        
		if(isEven.test(item.getProductId())) {
        	discountProduct.setDiscountAvailable(true);
        }
		return discountProduct;
	}

}
