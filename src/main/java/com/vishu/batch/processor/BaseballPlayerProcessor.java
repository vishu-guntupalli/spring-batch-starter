package com.vishu.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.vishu.batch.model.BaseballPlayer;

public class BaseballPlayerProcessor implements ItemProcessor<BaseballPlayer, BaseballPlayer>{

	public BaseballPlayer process(BaseballPlayer item) throws Exception {
		return item;
	}

}
