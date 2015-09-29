/**
 * Created by Pasin Suriyentrakorn on 8/25/15.
 * Copyright (c) 2015 Couchbase, Inc All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

#include <stdio.h>

#include "tomcrypt.h"

#include "com_couchbase_lite_support_security_SymmetricKey.h"

/**
 * Throw an exception of the given class name with the error code and message.
 */
static void throwException(JNIEnv* env, const char* className, int errCode, const char* message) {
  // Construct the exception message:
  char msg[1024];
  if (message != NULL) {
    sprintf(msg, "%s (code = %d)", message, errCode);
  } else {
    sprintf(msg, "(code = %d)", errCode);
  }

  // Throw the exception:
  jclass cls = env->FindClass(className);
  env->ThrowNew(cls, msg);
}

/**
 * Returns a derived PBKDF2-SHA256 key from a password with a given salt and the number of iterating rounds.
 */
JNIEXPORT jbyteArray JNICALL Java_com_couchbase_lite_support_security_SymmetricKey_nativeDeriveKey
  (JNIEnv* env, jclass clazz, jstring password, jbyteArray salt, jint rounds) {
    unsigned char output[32];
    unsigned long outputLen = sizeof(output);
    
    const char* passwordStr = env->GetStringUTFChars(password, NULL);
    unsigned long passwordLen = (unsigned long)strlen(passwordStr);
    
    int saltLen = env->GetArrayLength (salt);
    unsigned char* saltBytes = new unsigned char[saltLen];
    env->GetByteArrayRegion (salt, 0, saltLen, reinterpret_cast<jbyte*>(saltBytes));
    
    register_hash(&sha256_desc);
    int hash_id = find_hash(sha256_desc.name);

    jbyteArray result = NULL;
    int code = pkcs_5_alg2(reinterpret_cast<const unsigned char*>(passwordStr), passwordLen, 
                           saltBytes, saltLen, rounds, hash_id, output, &outputLen);
    if (code != CRYPT_OK) {
      // Error code definition are defined in the tomcrypt.h
      throwException(env,
                     "com/couchbase/lite/support/security/SymmetricKeyException", code,
                     "Cannot derive a key");
    } else {
      result = env->NewByteArray((int) outputLen);
      env->SetByteArrayRegion(result, 0, (int) outputLen, (jbyte *) output);
    }
    return result;
}
