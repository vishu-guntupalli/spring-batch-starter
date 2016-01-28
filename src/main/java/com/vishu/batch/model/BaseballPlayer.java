package com.vishu.batch.model;

public class BaseballPlayer {
	
	private String playerId;
	
	private String awardType;
	
	private int year;
	
	private String leagueType;
	
	private String isTie;
	
	private String notes;

	public String getPlayerId() {
		return playerId;
	}

	public void setPlayerId(String playerId) {
		this.playerId = playerId;
	}

	public String getAwardType() {
		return awardType;
	}

	public void setAwardType(String awardType) {
		this.awardType = awardType;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getLeagueType() {
		return leagueType;
	}

	public void setLeagueType(String leagueType) {
		this.leagueType = leagueType;
	}

	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}

	public String getIsTie() {
		return isTie;
	}

	public void setIsTie(String isTie) {
		this.isTie = isTie;
	}

}
