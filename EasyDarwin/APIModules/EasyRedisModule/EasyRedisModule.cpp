/*
	Copyright (c) 2012-2016 EasyDarwin.ORG.  All rights reserved.
	Github: https://github.com/EasyDarwin
	WEChat: EasyDarwin
	Website: http://www.easydarwin.org
*/
#include <stdio.h>

#include "EasyRedisModule.h"
#include "OSHeaders.h"
#include "QTSSModuleUtils.h"
#include "QTSServerInterface.h"
#include "ReflectorSession.h"
#include "EasyUtil.h"

#include "Windows/hiredis.h"
#include "Format.h"
#include "EasyRedisClient.h"

#include "Resources.h"

// STATIC VARIABLES
static QTSS_ModulePrefsObject	modulePrefs = nullptr;
static QTSS_PrefsObject			sServerPrefs = nullptr;
static QTSS_ServerObject		sServer = nullptr;

// Redis IP
static char*            sRedis_IP = nullptr;
static char*            sDefaultRedis_IP_Addr = "127.0.0.1";
// Redis Port
static UInt16			sRedisPort = 6379;
static UInt16			sDefaultRedisPort = 6379;
// Redis password
static char*            sRedisPassword = nullptr;
static char*            sDefaultRedisPassword = "admin";

static bool				sIfConSucess = false;
static OSMutex			sMutex;

static redisContext*	redisContext_ = nullptr;

static void RedisErrorHandler();

// FUNCTION PROTOTYPES
static QTSS_Error   EasyRedisModuleDispatch(QTSS_Role inRole, QTSS_RoleParamPtr inParamBlock);
static QTSS_Error   Register(QTSS_Register_Params* inParams);
static QTSS_Error   Initialize(QTSS_Initialize_Params* inParams);
static QTSS_Error   RereadPrefs();
static bool	RedisConnect();
static QTSS_Error	RedisTTL();
static QTSS_Error	RedisUpdateStream(Easy_StreamInfo_Params* inParams);
static QTSS_Error	RedisSetRTSPLoad();
static QTSS_Error	RedisGetAssociatedCMS(QTSS_GetAssociatedCMS_Params* inParams);
static QTSS_Error	RedisJudgeStreamID(QTSS_JudgeStreamID_Params* inParams);

class RedisReplyObjectDeleter
{
    public:
		RedisReplyObjectDeleter() : fReply(NULL) {}
        RedisReplyObjectDeleter(redisReply* reply) : fReply(reply)  {}
        ~RedisReplyObjectDeleter() 
		{ 
			if (fReply)
			{
				freeReplyObject(fReply);
			}
		}
        
        void ClearObject() { fReply = NULL; }

        void SetObject(redisReply* reply) 
        {
            fReply = reply; 
        }
        redisReply* GetObject() { return fReply; }
    
    private:
    
        redisReply* fReply;
};

QTSS_Error EasyRedisModule_Main(void* inPrivateArgs)
{
	return _stublibrary_main(inPrivateArgs, EasyRedisModuleDispatch);
}

QTSS_Error EasyRedisModuleDispatch(QTSS_Role inRole, QTSS_RoleParamPtr inParamBlock)
{
	switch (inRole)
	{
	case QTSS_Register_Role:
		return Register(&inParamBlock->regParams);
	case QTSS_Initialize_Role:
		return Initialize(&inParamBlock->initParams);
	case QTSS_RereadPrefs_Role:
		return RereadPrefs();
	case Easy_RedisTTL_Role:
		return RedisTTL();
	case Easy_RedisSetRTSPLoad_Role:
		return RedisSetRTSPLoad();
	case Easy_RedisUpdateStreamInfo_Role:
		return RedisUpdateStream(&inParamBlock->easyStreamInfoParams);
	case Easy_RedisGetAssociatedCMS_Role:
		return RedisGetAssociatedCMS(&inParamBlock->GetAssociatedCMSParams);
	case Easy_RedisJudgeStreamID_Role:
		return RedisJudgeStreamID(&inParamBlock->JudgeStreamIDParams);
	default: break;
	}
	return QTSS_NoErr;
}

QTSS_Error Register(QTSS_Register_Params* inParams)
{
	// Do role setup
	(void)QTSS_AddRole(QTSS_Initialize_Role);
	(void)QTSS_AddRole(QTSS_RereadPrefs_Role);
	(void)QTSS_AddRole(Easy_RedisTTL_Role);
	(void)QTSS_AddRole(Easy_RedisSetRTSPLoad_Role);
	(void)QTSS_AddRole(Easy_RedisUpdateStreamInfo_Role);
	(void)QTSS_AddRole(Easy_RedisGetAssociatedCMS_Role);
	(void)QTSS_AddRole(Easy_RedisJudgeStreamID_Role);

	// Tell the server our name!
	static char* sModuleName = "EasyRedisModule";
	::strcpy(inParams->outModuleName, sModuleName);

	return QTSS_NoErr;
}

QTSS_Error Initialize(QTSS_Initialize_Params* inParams)
{
	QTSSModuleUtils::Initialize(inParams->inMessages, inParams->inServer, inParams->inErrorLogStream);
	sServer = inParams->inServer;
	sServerPrefs = inParams->inPrefs;
	modulePrefs = QTSSModuleUtils::GetModulePrefsObject(inParams->inModule);

	RereadPrefs();

	RedisConnect();

	return QTSS_NoErr;
}

QTSS_Error RereadPrefs()
{
	delete[] sRedis_IP;
	sRedis_IP = QTSSModuleUtils::GetStringAttribute(modulePrefs, "redis_ip", sDefaultRedis_IP_Addr);

	QTSSModuleUtils::GetAttribute(modulePrefs, "redis_port", qtssAttrDataTypeUInt16, &sRedisPort, &sDefaultRedisPort, sizeof(sRedisPort));

	delete[] sRedisPassword;
	sRedisPassword = QTSSModuleUtils::GetStringAttribute(modulePrefs, "redis_password", sDefaultRedisPassword);

	return QTSS_NoErr;
}

bool RedisConnect()
{
	if (sIfConSucess)
	{
		return true;
	}
	
	bool theRet = false;
	do
	{
		struct timeval timeout = { 2, 0 }; // 2 seconds
		redisContext_ = redisConnectWithTimeout(sRedis_IP, sRedisPort, timeout);
		if (!redisContext_ || redisContext_->err)
		{
			if (redisContext_)
			{
				printf("Redis context connect error \n");
			}
			else
			{
				printf("Connection error: can't allocate redis context\n");
			}

			theRet = false;
			break;
		}

		string auth = Format("auth %s", string(sRedisPassword));
		redisReply* reply = static_cast<redisReply*>(redisCommand(redisContext_, auth.c_str()));

		RedisReplyObjectDeleter replyDeleter(reply);
		if (!reply || string(reply->str) != string("OK"))
		{
			printf("Redis auth error\n");
			theRet = false;
			break;
		}

		theRet = true;
		sIfConSucess = true;
	
		printf("Connect Redis success\n");

	}while(0);

	if (!theRet && redisContext_)
	{
		RedisErrorHandler();
	}

	return theRet;
}

QTSS_Error RedisTTL()
{
	OSMutexLocker mutexLock(&sMutex);

	if (!RedisConnect())
	{
		return QTSS_NotConnected;
	}
	QTSS_Error theRet = QTSS_NoErr;

do
{
	auto server = QTSServerInterface::GetServerName().Ptr;
	auto guid = QTSServerInterface::GetServer()->GetCloudServiceNodeID();
	auto load = QTSServerInterface::GetServer()->GetNumRTPSessions();

	char chKey[512] = { 0 };
	sprintf(chKey, "expire %s:%s 15", server, guid);
	auto reply = static_cast<redisReply*>(redisCommand(redisContext_, chKey));

	RedisReplyObjectDeleter replyDeleter(reply);

	if (!reply)
	{
		printf("Redis expire EasyDarwin error\n"); 
		theRet = QTSS_NotConnected;
		break;
	}

	if (reply->integer == 0)
	{
		auto ip = QTSServerInterface::GetServer()->GetPrefs()->GetServiceWANIP();
		auto http = QTSServerInterface::GetServer()->GetPrefs()->GetServiceWanPort();
		auto rtsp = QTSServerInterface::GetServer()->GetPrefs()->GetRTSPWANPort();

		sprintf(chKey, "hmset %s:%s %s %s %s %d %s %d %s %d", server, guid, EASY_REDIS_IP, ip, EASY_REDIS_HTTP,
			http, EASY_REDIS_RTSP, rtsp, EASY_REDIS_LOAD, load);
		auto replyHmset = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
		RedisReplyObjectDeleter replyHmsetDeleter(replyHmset);

		if (!replyHmset)
		{
			printf("Redis hmset EasyDarwin error\n");
			theRet = QTSS_NotConnected;
			break;
		}

		sprintf(chKey, "expire %s:%s 15", server, guid);
		auto replyExpire = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
		RedisReplyObjectDeleter replyExpireDeleter(replyExpire);

		if (!replyExpire)
		{
			printf("Redis expire new EasyDarwin error\n");
			theRet = QTSS_NotConnected;
			break;
		}
	}
	else if (reply->integer == 1)
	{
		//TODO::nothing
	}

}while(0);

	if (theRet != QTSS_NoErr)
	{
		RedisErrorHandler();
	}

	return QTSS_NoErr;
}

QTSS_Error RedisUpdateStream(Easy_StreamInfo_Params* inParams)
{
	OSMutexLocker mutexLock(&sMutex);

	if (!RedisConnect())
	{
		return QTSS_NotConnected;
	}
	QTSS_Error theRet = QTSS_NoErr;

do
{
	char chKey[128] = { 0 };
	sprintf(chKey, "del %s:%s/%d", EASY_REDIS_LIVE, inParams->inStreamName, inParams->inChannel);

	if (inParams->inAction == easyRedisActionDelete)
	{
		auto reply = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
		RedisReplyObjectDeleter replyDeleter(reply);

		if (!reply)
		{
			printf("Redis del Live error\n");
			theRet = QTSS_NotConnected;
			break;
		}
		return QTSS_NoErr;
	}

	sprintf(chKey, "hmset %s:%s/%d %s %d %s %d %s %s", EASY_REDIS_LIVE, inParams->inStreamName, inParams->inChannel,
		EASY_REDIS_BITRATE, inParams->inBitrate, EASY_REDIS_OUTPUT, inParams->inNumOutputs,
		EASY_REDIS_EASYDARWIN, QTSServerInterface::GetServer()->GetCloudServiceNodeID());
	auto reply = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
	RedisReplyObjectDeleter replyDeleter(reply);

	if (!reply)
	{
		printf("Redis hmset Live error\n");
		theRet = QTSS_NotConnected;
		break;
	}

	if (string(reply->str) == string("OK"))
	{
		sprintf(chKey, "expire %s:%s/%d 150", EASY_REDIS_LIVE, inParams->inStreamName, inParams->inChannel);
		auto replyExpire = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
		RedisReplyObjectDeleter replyExpireDeleter(replyExpire);

		if (!replyExpire)
		{
			printf("Redis expire Live error\n");

			theRet = QTSS_NotConnected;
			break;
		}
	}
	else
	{
		theRet = QTSS_NotConnected;
		break;
	}

}while(0);

	if (theRet != QTSS_NoErr)
	{
		RedisErrorHandler();
	}

	return QTSS_NoErr;
}

QTSS_Error RedisGetAssociatedCMS(QTSS_GetAssociatedCMS_Params* inParams)
{
	OSMutexLocker mutexLock(&sMutex);

	if (!RedisConnect())
	{
		return QTSS_NotConnected;
	}

	QTSS_Error theRet = QTSS_NoErr;

do
{
	string exists = Format("exists %s:%s", string(EASY_REDIS_DEVICE), string(inParams->inSerial));
	auto reply = static_cast<redisReply*>(redisCommand(redisContext_, exists.c_str()));
	RedisReplyObjectDeleter replyDeleter(reply);

	if (!reply)
	{
		printf("Redis exists Device error\n");
		theRet = QTSS_NotConnected;
		break;
	}

	if (reply->integer == 1)
	{
		string strTemp = Format("hmget %s:%s %s", string(EASY_REDIS_DEVICE), string(inParams->inSerial),
			string(EASY_REDIS_EASYCMS));
		auto replyHmget = static_cast<redisReply*>(redisCommand(redisContext_, strTemp.c_str()));
		RedisReplyObjectDeleter replyHmgetDeleter(replyHmget);

		if (!replyHmget)
		{
			printf("Redis hmget Device error\n");
			theRet = QTSS_NotConnected;
			break;
		}

		string easycms = Format("%s:", string(EASY_REDIS_EASYCMS));
		easycms += replyHmget->element[0]->str;

		strTemp = Format("hmget %s %s %s ", easycms, string(EASY_REDIS_IP), string(EASY_REDIS_PORT));
		auto replyHmgetEasyDarwin = static_cast<redisReply*>(redisCommand(redisContext_, strTemp.c_str()));
		RedisReplyObjectDeleter replyHmgetEasyDarwinDeleter(replyHmgetEasyDarwin);

		if (!replyHmgetEasyDarwin)
		{
			printf("Redis hmget EasyCMS error\n");

			theRet = QTSS_NotConnected;
			break;
		}

		if (replyHmgetEasyDarwin->type == EASY_REDIS_REPLY_NIL)
		{
			theRet = QTSS_RequestFailed;
			break;
		}

		if (replyHmgetEasyDarwin->type == EASY_REDIS_REPLY_ARRAY && replyHmgetEasyDarwin->elements == 2)
		{
			bool ok = true;
			for (int i = 0; i < replyHmgetEasyDarwin->elements; ++i)
			{
				if (replyHmgetEasyDarwin->element[i]->type == EASY_REDIS_REPLY_NIL)
				{
					ok = ok && false;
				}
			}

			if (ok)
			{
				memcpy(inParams->outCMSIP, replyHmgetEasyDarwin->element[0]->str, replyHmgetEasyDarwin->element[0]->len);
				memcpy(inParams->outCMSPort, replyHmgetEasyDarwin->element[1]->str, replyHmgetEasyDarwin->element[1]->len);
			}
			else
			{
				theRet = QTSS_RequestFailed;
				break;
			}
		}
	}

}while(0);

	if (theRet != QTSS_NoErr)
	{
		RedisErrorHandler();
	}
	return theRet;
}

QTSS_Error RedisSetRTSPLoad()
{
	OSMutexLocker mutexLock(&sMutex);

	if (!RedisConnect())
	{
		return QTSS_NotConnected;
	}
	QTSS_Error theRet = QTSS_NoErr;

do
{
	char chKey[128] = { 0 };
	auto server = QTSServerInterface::GetServer()->GetServerName().Ptr;
	auto guid = QTSServerInterface::GetServer()->GetCloudServiceNodeID();
	auto load = QTSServerInterface::GetServer()->GetNumRTPSessions();

	sprintf(chKey, "expire %s:%s 15", server, guid);
	auto reply = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
	RedisReplyObjectDeleter replyDeleter(reply);

	if (!reply)
	{
		printf("Redis expire EasyDarwin error\n");
		theRet = QTSS_NotConnected;
		break;
	}

	if (reply->integer == 1)
	{
		sprintf(chKey, "hset %s:%s %s %d", server, guid, EASY_REDIS_LOAD, load);
		auto replyHset = static_cast<redisReply*>(redisCommand(redisContext_, chKey));
		RedisReplyObjectDeleter replyHsetDeleter(replyHset);

		if (!replyHset)
		{
			printf("Redis hset EasyDarwin Load error\n");
			theRet = QTSS_NotConnected;
			break;
		}
	}
}while(0);

	if (theRet != QTSS_NoErr)
	{
		RedisErrorHandler();
	}
	return theRet;
}

QTSS_Error RedisJudgeStreamID(QTSS_JudgeStreamID_Params* inParams)
{
	////算法描述，删除指定sessionID对应的key，如果成功删除，表明SessionID存在，验证通过，否则验证失败
	//OSMutexLocker mutexLock(&sMutex);
	//if (!sIfConSucess)
	//	return QTSS_NotConnected;

	//char chKey[128] = { 0 };
	//sprintf(chKey, "SessionID_%s", inParams->inStreanID);//如果key存在则返回整数类型1，否则返回整数类型0

	//int ret = sRedisClient->Delete(chKey);

	//if (ret == -1)//fatal err,need reconnect
	//{
	//	sRedisClient->Free();
	//	sIfConSucess = false;

	//	return QTSS_NotConnected;
	//}
	//else if (ret == 0)
	//{
	//	*(inParams->outresult) == 1;
	//	return QTSS_NoErr;
	//}
	//else
	//{
	//	return ret;
	//}
	return 0;
}

static void RedisErrorHandler()
{
	sIfConSucess = false;
	if(redisContext_)
	{
		printf("Connection error: %s\n", redisContext_->errstr);
		redisFree(redisContext_);
	}
	redisContext_ = NULL;
}