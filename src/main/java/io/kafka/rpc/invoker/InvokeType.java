/*
 * Copyright (c) 2015 The Jupiter Project
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kafka.rpc.invoker;

/**
 * Զ�̵��÷�ʽ, ֧��ͬ�����ú��첽����, �첽��ʽ֧��Future�Լ�Listener.
 *
 */
public enum InvokeType {
	SYNC,   // ͬ������
    ASYNC;  // �첽����

    public static InvokeType parse(String name) {
        for (InvokeType s : values()) {
            if (s.name().equalsIgnoreCase(name)) {
                return s;
            }
        }
        return null;
    }

    public static InvokeType getDefault() {
        return SYNC;
    }
}
