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

CREATE TABLE SimpleContacts
(
    row_id         INTEGER,
    person_name    TEXT,
    contact_type   TEXT,
    contact_number TEXT
);
INSERT INTO SimpleContacts (row_id, person_name, contact_type, contact_number)
VALUES (1, 'sunny meet', 'WORK', '(559)458-7172'),
       (2, 'baggy direction', 'WORK', '(322)466-8413'),
       (3, 'vast influence', 'WORK', '(571)861-3996'),
       (4, 'proper method', 'WORK', '(937)372-5515'),
       (5, 'damaged drink', 'WORK', '(291)529-9401')