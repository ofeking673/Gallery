#include <map>
#include <algorithm>
#include "ItemNotFoundException.h"
#include "DatabaseAccess.h"
#include <io.h>

int assemblePic(void* data, int argc, char** argv, char** azColName)
{
	Album* alb = (Album*)data;
	Picture* pic = new Picture(1, "1");
	pic->setId(std::atoi(argv[0]));
	pic->setName(argv[1]);
	pic->setPath(argv[2]);
	pic->setCreationDate(argv[3]);
	alb->addPicture(*pic);
	return 0;
}

int assembleTags(void* data, int argc, char** argv, char** azColName) {
	std::set<int>* tags = (std::set<int>*)data;
	tags->insert(std::atoi(argv[0]));
	return 0;
}

void DatabaseAccess::getAlbPics(Album* alb)
{
	std::string msg = "SELECT * FROM PICTURES WHERE ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME ='" + alb->getName() + "')";
	sqlite3_exec(db, msg.c_str(), assemblePic, alb, nullptr);

	auto pics = alb->getPicturesPointer();
	for (auto it = pics->begin(); it != pics->end(); it++)
	{
		getPicTags(&*it);
	}
}

int printAlb(void* data, int argc, char** argv, char** azColName)
{
	for (int i = 0; i < argc; i++)
	{
		std::cout << argv[i] << ", ";
	}
	std::cout << std::endl;
	return 0;
}

void DatabaseAccess::printAlbums()
{
	std::string msg = "SELECT * FROM ALBUMS"; // 7
	sqlite3_exec(db, msg.c_str(), printAlb , nullptr, nullptr);
}

bool DatabaseAccess::open()
{
	std::string dbName = "galleryDB.sqlite";
	int answer = _access(dbName.c_str(), 0);
	int res = sqlite3_open(dbName.c_str(), &db);

	if (answer == -1)
	{
		sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS USERS(ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, NAME TEXT NOT NULL);", nullptr, nullptr, nullptr);

		sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS ALBUMS(ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, NAME TEXT NOT NULL, CREATION_DATE DATE NOT NULL, USER_ID INT NOT NULL, FOREIGN KEY(USER_ID) REFERENCES USERS(ID));", nullptr, nullptr, nullptr);

		sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS PICTURES(ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, NAME TEXT NOT NULL, LOCATION TEXT NOT NULL, CREATION_DATE DATE NOT NULL, ALBUM_ID INT NOT NULL, FOREIGN KEY(ALBUM_ID) REFERENCES ALBUMS(ID));", nullptr, nullptr, nullptr);

		sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS TAGS(ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, USER_ID INT NOT NULL, PICTURE_ID INT NOT NULL, FOREIGN KEY(PICTURE_ID) REFERENCES PICTURES(ID), FOREIGN KEY(USER_ID) REFERENCES USER(ID));", nullptr, nullptr, nullptr);
	}
	

	return true;
}

void DatabaseAccess::clear() {
	sqlite3_db_config(db, SQLITE_DBCONFIG_RESET_DATABASE, 1, 0);
	sqlite3_exec(db, "VACUUM", 0, 0, 0);
	sqlite3_db_config(db, SQLITE_DBCONFIG_RESET_DATABASE, 0, 0);
}

int getAlbum(void* data, int argc, char** argv, char** azColName) {
	Album* ok = (Album*)data;
	ok->setName(argv[1]);
	ok->setCreationDate(argv[3]);
	ok->setOwner(std::atoi(argv[2]));

	return 0;
}
auto DatabaseAccess::getAlbumIfExists(const std::string & albumName)
{
	Album retAlbum;
	std::string msg = "SELECT * FROM ALBUMS WHERE NAME = " + albumName;
	sqlite3_exec(db, msg.c_str(), getAlbum, &retAlbum, nullptr);
	getAlbPics(&retAlbum);
	return retAlbum;
}

int analyzeAlbums(void* data, int argc, char** argv, char** azColName)
{
	std::list<Album>* lst = static_cast<std::list<Album>*>(data); //data = lst
	Album* alb = new Album();

	for (int i = 0; i < argc; i++)
	{
		if (std::string(azColName[i]) == "ID")
		{}
		else if (std::string(azColName[i]) == "NAME")
		{
			alb->setName(argv[i]);
		}
		else if (std::string(azColName[i]) == "USER_ID")
		{
			alb->setOwner(std::atoi(argv[i]));
		}
		else
		{
			alb->setCreationDate(argv[i]);
		}
	}
	lst->push_back(*alb);
	return 0;
}

const std::list<Album> DatabaseAccess::getAlbums() 
{
	std::list<Album> lst;
	sqlite3_exec(db, "SELECT * FROM ALBUMS", analyzeAlbums, &lst, nullptr);

	for each (Album var in lst)
	{
		getAlbPics(&var);
	}

	return lst;
}

const std::list<Album> DatabaseAccess::getAlbumsOfUser(const User& user) 
{	
	std::list<Album> albumsOfUser;
	std::string msg = "SELECT * FROM ALBUMS WHERE USER_ID =" + std::to_string(user.getId());
	sqlite3_exec(db, msg.c_str(), analyzeAlbums, &albumsOfUser, nullptr);
	for each (Album var in albumsOfUser)
	{
		getAlbPics(&var);
	}

	return albumsOfUser;
}

void DatabaseAccess::createAlbum(const Album& album)
{
	std::string msg = "INSERT INTO ALBUMS (NAME, USER_ID, CREATION_DATE) VALUES('" + album.getName() + "', " + std::to_string(album.getOwnerId()) + ", '" + album.getCreationDate() + "')";
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

void DatabaseAccess::deleteAlbum(const std::string& albumName, int userId)
{
	std::string msg = "DELETE FROM ALBUMS WHERE NAME='" + albumName + "'AND USER_ID = " + std::to_string(userId);
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

int doesExist(void* data, int argc, char** argv, char** azColName) {
	bool* test = (bool*)data;
	*test = true;
	return 0;
}

bool DatabaseAccess::doesAlbumExists(const std::string& albumName, int userId) 
{
	bool ok = false;
	std::string msg = "SELECT USER_ID FROM ALBUMS WHERE NAME = '" + albumName + "' AND USER_ID = " + std::to_string(userId);
	sqlite3_exec(db, msg.c_str(), doesExist, &ok, nullptr);

	return ok;
}

Album DatabaseAccess::openAlbum(const std::string& albumName) 
{
	openAlbumName = albumName;
	Album alb;
	std::string msg = "SELECT * FROM ALBUMS WHERE NAME='" + albumName + "'";
	sqlite3_exec(db, msg.c_str(), getAlbum, &alb, nullptr);
	getAlbPics(&alb);
	return alb;
}

void DatabaseAccess::addPictureToAlbumByName(const std::string& albumName, const Picture& picture) 
{
	/*
	int m_pictureId;
	std::string m_name;
	std::string m_pathOnDisk;
	std::string m_creationDate;
	std::set<int> m_usersTags;
	*/
	Album alb;
	std::string msg = "INSERT INTO PICTURES(NAME, LOCATION, CREATION_DATE, ALBUM_ID) VALUES('" + picture.getName() + "', '" + picture.getPath() + "', '" + picture.getCreationDate() + "', (SELECT ID FROM ALBUMS WHERE NAME='" + albumName + "'))";
	std::cout << msg;
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

void DatabaseAccess::removePictureFromAlbumByName(const std::string& albumName, const std::string& pictureName) 
{
	std::string msg = "DELETE FROM PICTURES WHERE ID = " + getPicId(pictureName, albumName);
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

int getPicIdHelp(void* data, int argc, char** argv, char** azColName)
{
	std::string* picId = (std::string*)data;
	*picId = argv[0];
	return 0;
}

std::string DatabaseAccess::getPicId(std::string picName, std::string albumName)
{
	std::string ret;
	std::string msg = "SELECT ID FROM PICTURES WHERE NAME = '" + picName + "' AND ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME ='" + albumName + "')";
	sqlite3_exec(db, msg.c_str(), getPicIdHelp, &ret, nullptr);
	return ret;
}

void DatabaseAccess::tagUserInPicture(const std::string& albumName, const std::string& pictureName, int userId)
{
	std::string msg = "INSERT INTO TAGS(PICTURE_ID, USER_ID) VALUES(" + getPicId(pictureName, albumName) + ", " + std::to_string(userId) + ")";
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

void DatabaseAccess::untagUserInPicture(const std::string& albumName, const std::string& pictureName, int userId)
{
	std::string msg = "DELETE FROM TAGS WHERE PICTURE_ID = " + getPicId(pictureName, albumName) + " AND USER_ID = " + std::to_string(userId);
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

void DatabaseAccess::getPicTags(Picture* pic)
{
	std::set<int> tags;
	std::string msg = "SELECT USER_ID FROM TAGS WHERE PICTURE_ID = " + std::to_string(pic->getId());
	sqlite3_exec(db, msg.c_str(), assembleTags, &tags, nullptr);

	for each (int tag in tags)
	{
		pic->tagUser(tag);
	}
}

void DatabaseAccess::closeAlbum(Album& pAlbum)
{
	openAlbumName = "";
}

// ******************* User ******************* 
void DatabaseAccess::printUsers()
{
	std::string msg = "SELECT * FROM USERS";
	sqlite3_exec(db, msg.c_str(), printAlb, nullptr, nullptr);
}

int UserHelp(void* data, int argc, char** argv, char** azColName)
{
	User* usr = (User*)data;
	usr->setId(std::atoi(argv[0]));
	usr->setName(argv[1]);
	return 0;
}

User DatabaseAccess::getUser(int userId) {
	User usr(1,"1");
	std::string msg = "SELECT * FROM USERS WHERE ID=" + std::to_string(userId);
	sqlite3_exec(db, msg.c_str(), UserHelp, &usr, nullptr);
	return usr;
}

void DatabaseAccess::createUser(User& user)
{
	std::string msg = "INSERT INTO USERS(NAME) VALUES('" + user.getName() + "')";
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

void DatabaseAccess::deleteUser(const User& user)
{
	std::string msg = "DELETE FROM USERS WHERE ID = " + std::to_string(user.getId());
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, nullptr);
}

bool DatabaseAccess::doesUserExists(int userId) 
{
	std::string msg = "SELECT * FROM USERS WHERE ID = " + std::to_string(userId);
	char* err = new char[1000];
	sqlite3_exec(db, msg.c_str(), nullptr, nullptr, &err);
	if (err == nullptr)
	{
		delete[] err;
		return true;
	}
	
	delete[] err;
	return false;
}

int getAlbumCount(void* data, int argc, char** argv, char** azColName)
{
	for (int i = 0; i < argc; i++)
	{
		if (azColName[i] == "COUNT(USER_ID)")
		{
			data = argv[i];
		}
	}
	return 0;
}
// user statistics
int DatabaseAccess::countAlbumsOwnedOfUser(const User& user) 
{
	int albumsCount = 0;
	char* val = new char[10];
	std::string msg = "SELECT USER_ID, COUNT(USER_ID) FROM ALBUMS WHERE USER_ID = " + user.getId();
	sqlite3_exec(db, msg.c_str(), getAlbumCount, val, nullptr);
	albumsCount = std::atoi(val);

	return albumsCount;
}

int getAlbumTags(void* data, int argc, char** argv, char** azColName)
{
	for (int i = 0; i < argc; i++)
	{
		if (azColName[i] == "COUNT()")
		{
			data = argv[i];
		}
	}
	return 0;
}

int count(void* data, int argc, char** argv, char** azColName)
{
	int* Idata = (int*)data;
	*Idata += 1;
	return 0;
}

int DatabaseAccess::countAlbumsTaggedOfUser(const User& user) 
{
	int albumsCount = 0;
	std::string msg = "SELECT DISTINCT ALBUMS.NAME FROM TAGS INNER JOIN PICTURES ON PICTURES.ID = TAGS.PICTURE_ID INNER JOIN ALBUMS ON ALBUMS.ID = PICTURES.ALBUM_ID WHERE TAGS.USER_ID = " + std::to_string(user.getId());
	sqlite3_exec(db, msg.c_str(), count, &albumsCount, nullptr);

	return albumsCount;
}

int getTags(void* data, int argc, char** argv, char** azColName)
{
	int* Idata = (int*)data;
	*Idata = std::atoi(argv[0]);
	return 0;
}

int DatabaseAccess::countTagsOfUser(const User& user) 
{
	int tagsCount = 0;
	std::string msg = "SELECT COUNT(USER_ID) FROM TAGS WHERE USER_ID = " + std::to_string(user.getId());
	sqlite3_exec(db, msg.c_str(), getTags, &tagsCount, nullptr);

	return tagsCount;
}

float DatabaseAccess::averageTagsPerAlbumOfUser(const User& user) 
{
	int albumsTaggedCount = countAlbumsTaggedOfUser(user);

	if ( 0 == albumsTaggedCount ) {
		return 0;
	}

	return static_cast<float>(countTagsOfUser(user)) / albumsTaggedCount;
}

int topTag(void* data, int argc, char** argv, char** azColName)
{
	int* Idata = (int*)data;
	*Idata = std::atoi(argv[0]);
	return 0;
}

User DatabaseAccess::getTopTaggedUser()
{
	int userId;
	std::string msg = "SELECT USER_ID, COUNT(USER_ID) FROM TAGS GROUP BY USER_ID ORDER BY COUNT(USER_ID)";
	sqlite3_exec(db, msg.c_str(), topTag, &userId, nullptr);

	return getUser(userId);
}

int analyzePic(void* data, int argc, char** argv, char** azColName)
{
	Picture* pic = (Picture*)data;
	pic->setId(std::atoi(argv[0]));
	pic->setName(argv[1]);
	pic->setPath(argv[2]);
	pic->setCreationDate(argv[3]);
	return 0;
}

Picture DatabaseAccess::getTopTaggedPicture()
{
	Picture retPic(1,"1");
	std::string msg = "SELECT * FROM PICTURES WHERE ID= (SELECT PICTURE_ID FROM TAGS GROUP BY PICTURE_ID ORDER BY COUNT(PICTURE_ID) DESC LIMIT 0,1)";
	sqlite3_exec(db, msg.c_str(), analyzePic, &retPic, nullptr);

	return retPic;
}

int getTaggedPics(void* data, int argc, char** argv, char** azColName)
{
	std::list<Picture>* lst = (std::list<Picture>*)data;
	Picture* pic = new Picture(std::atoi(argv[0]), argv[1], argv[2], argv[3]);
	lst->push_back(*pic);
	return 0;
}

std::list<Picture> DatabaseAccess::getTaggedPicturesOfUser(const User& user)
{
	std::list<Picture> lst;
	std::string msg = "SELECT PICTURES.ID, PICTURES.NAME, PICTURES.LOCATION, PICTURES.CREATION_DATE FROM TAGS INNER JOIN PICTURES ON PICTURES.ID = TAGS.PICTURE_ID WHERE TAGS.USER_ID =" + std::to_string(user.getId());
	sqlite3_exec(db, msg.c_str(), getTaggedPics, &lst, nullptr);

	for (auto it = lst.begin(); it != lst.end(); it++)
	{
		getPicTags(&*it);
	}

	return lst;
}
