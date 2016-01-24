package com.vishu.batch.model;

public class DiscountProduct extends Product {
	
	public DiscountProduct() {
		super();
	}

	public DiscountProduct(Product product) {
		super(product.getProductId(), product.getProductName(), product.getPrice());
	}
	
	private boolean discountAvailable;

	public boolean isDiscountAvailable() {
		return discountAvailable;
	}

	public void setDiscountAvailable(boolean discountAvailable) {
		this.discountAvailable = discountAvailable;
	}

}
