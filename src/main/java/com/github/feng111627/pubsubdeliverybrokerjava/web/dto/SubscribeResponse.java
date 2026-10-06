package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

public record SubscribeResponse(String clientId, String filter, long position) { }
