LOCAL_PATH := $(call my-dir)

### Build libtomcrypt ###

include $(CLEAR_VARS)
LOCAL_MODULE := libtomcrypt
LOCAL_C_INCLUDES := $(LOCAL_PATH)/../../../vendor/libtomcrypt/src/headers
LIBTOMCRYPT_PATH := ../../../vendor/libtomcrypt
LOCAL_SRC_FILES := $(LIBTOMCRYPT_PATH)/src/hashes/helper/hash_memory.c \
                   $(LIBTOMCRYPT_PATH)/src/hashes/sha2/sha256.c \
                   $(LIBTOMCRYPT_PATH)/src/mac/hmac/hmac_init.c \
                   $(LIBTOMCRYPT_PATH)/src/mac/hmac/hmac_memory.c \
                   $(LIBTOMCRYPT_PATH)/src/mac/hmac/hmac_process.c \
                   $(LIBTOMCRYPT_PATH)/src/mac/hmac/hmac_done.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/zeromem.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/crypt/crypt_find_hash.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/crypt/crypt_register_hash.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/crypt/crypt_argchk.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/crypt/crypt_hash_is_valid.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/crypt/crypt_hash_descriptor.c \
                   $(LIBTOMCRYPT_PATH)/src/misc/pkcs5/pkcs_5_2.c
include $(BUILD_STATIC_LIBRARY)

### Build CouchbaseLiteJavaSymmetricKey ###

include $(CLEAR_VARS)
LOCAL_MODULE := CouchbaseLiteJavaSymmetricKey
LOCAL_C_INCLUDES += $(LOCAL_PATH)/../../../vendor/libtomcrypt/src/headers
LOCAL_C_INCLUDES += $(LOCAL_PATH)/headers
LOCAL_SRC_FILES := source/com_couchbase_lite_support_security_SymmetricKey.cpp
LOCAL_STATIC_LIBRARIES := libtomcrypt
LOCAL_CPPFLAGS := -DANDROID_LOG
LOCAL_LDLIBS := -llog
include $(BUILD_SHARED_LIBRARY)
