﻿#pragma once
#include <list>
#include "Album.h"
#include "User.h"
#include "IDataAccess.h"


class DatabaseAccess : public IDataAccess
{

public:
	DatabaseAccess() = default;
	virtual ~DatabaseAccess() = default;

	// album related
	const std::list<Album> getAlbums() override;
	const std::list<Album> getAlbumsOfUser(const User& user) override;
	void createAlbum(const Album& album) override;
	void deleteAlbum(const std::string& albumName, int userId) override;
	bool doesAlbumExists(const std::string& albumName, int userId) override;
	Album openAlbum(const std::string& albumName) override;
	void closeAlbum(Album &pAlbum) override;
	void printAlbums() override;
	void getAlbPics(Album* alb);

	// picture related
	std::string getPicId(std::string picName, std::string albumName);
	void addPictureToAlbumByName(const std::string& albumName, const Picture& picture) override;
	void removePictureFromAlbumByName(const std::string& albumName, const std::string& pictureName) override;
	void tagUserInPicture(const std::string& albumName, const std::string& pictureName, int userId) override;
	void untagUserInPicture(const std::string& albumName, const std::string& pictureName, int userId) override;
	void getPicTags(Picture* pic);

	// user related
	void printUsers() override;
	void createUser(User& user) override;
	void deleteUser(const User& user) override;
	bool doesUserExists(int userId) override;
	User getUser(int userId) override;

	// user statistics
	int countAlbumsOwnedOfUser(const User& user) override;
    int countAlbumsTaggedOfUser(const User& user) override;
	int countTagsOfUser(const User& user) override;
	float averageTagsPerAlbumOfUser(const User& user) override;

	// queries
	User getTopTaggedUser() override;
	Picture getTopTaggedPicture() override;
	std::list<Picture> getTaggedPicturesOfUser(const User& user) override;

	bool open() override;
	void close() override { sqlite3_close(db); db = nullptr; };
	void clear() override;

private:
	sqlite3* db;
	std::string openAlbumName;

	auto getAlbumIfExists(const std::string& albumName);
};
