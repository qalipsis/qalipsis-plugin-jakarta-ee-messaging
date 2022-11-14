/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.jakarta



/**
 *
 * @author Alexander Sosnovsky
 */
object Constants {
    //const val DOCKER_IMAGE = "rmohr/activemq:5.15.9"
    const val DOCKER_IMAGE = "vromero/activemq-artemis"
    const val CONTAINER_USER_NAME = "qalipsis_user"
    const val CONTAINER_USER_NAME_ENV_KEY = "ARTEMIS_USERNAME"
    const val CONTAINER_PASSWORD = "qalipsis_password"
    const val CONTAINER_PASSWORD_ENV_KEY = "ARTEMIS_PASSWORD"

    const val brokerUrl = "tcp://localhost:61616"
}