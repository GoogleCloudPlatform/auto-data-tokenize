/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE SimpleFlatRecords
(
    id                    INTEGER AUTO_INCREMENT PRIMARY KEY,
    name                  TEXT,
    regional_name         VARCHAR(50) CHARACTER SET 'utf8mb4',
    salary                DOUBLE,
    birth_date            DATE,
    last_signin_timestamp TIMESTAMP
);
INSERT INTO SimpleFlatRecords(id, name, regional_name, salary, birth_date, last_signin_timestamp)
VALUES (1, 'Test User1', 'टेस्ट यूजर1', 123.2453, '2000-05-11', '2021-05-11 23:55:00'),
       (2, 'Test User2', '测试用户2', 456.7896, '2001-06-12', '2020-05-11 13:55:00'),
       (3, 'Test User3', '「テストユーザー」3', 789.1225, '2002-07-13', '2019-07-15 12:55:00');