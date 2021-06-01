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

package com.google.cloud.solutions.autotokenize.common;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth8;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ZippingIteratorTest {

  @Test
  public void create_singleList_valid() {
    var list1 = ImmutableList.of("element1", "element2", "element3");
    var expectedList1 =
        list1.stream().map(x -> ImmutableList.of(Optional.of(x))).collect(toImmutableList());
    var zippingIterator = ZippingIterator.create(ImmutableList.of(list1));

    var zippedList = ImmutableList.copyOf(zippingIterator);

    assertThat(zippedList).hasSize(3);
    assertThat(zippedList).containsExactlyElementsIn(expectedList1);
  }

  @Test
  public void create_twoLists_valid() {

    var list1 = ImmutableList.of("element1", "element2", "element3");
    var list2 = ImmutableList.of("element4", "element5", "element6");

    var zippingIterator = ZippingIterator.create(ImmutableList.of(list1, list2));

    int i = 0;

    while (zippingIterator.hasNext()) {
      var item = zippingIterator.next();

      assertThat(item.get(0)).isEqualTo(Optional.of(list1.get(i)));
      assertThat(item.get(1)).isEqualTo(Optional.of(list2.get(i)));
      i++;
    }

    assertThat(i).isEqualTo(3);
  }

  @Test
  public void create_twoUnEqualLists_valid() {

    var list1 = ImmutableList.of("element1", "element2", "element3");
    var list2 = ImmutableList.of("element4", "element5");

    var zippingIterator = ZippingIterator.create(ImmutableList.of(list1, list2));

    int i = 0;

    while (zippingIterator.hasNext()) {
      var item = zippingIterator.next();

      assertThat(item.get(0).get()).isEqualTo(list1.get(i));

      if (i < list2.size()) {
        assertThat(item.get(1).get()).isEqualTo(list2.get(i));
      } else {
        Truth8.assertThat(item.get(1)).isEmpty();
      }

      i++;
    }

    assertThat(i).isEqualTo(3);
  }
}
