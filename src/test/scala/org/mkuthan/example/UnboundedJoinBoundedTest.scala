// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.mkuthan.example

import com.spotify.scio.bigquery.BigQueryIO
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.testStreamOf
import org.mkuthan.example.UnboundedJoinBounded.Author
import org.mkuthan.example.UnboundedJoinBounded.Book

object UnboundedJoinBoundedTest {
  val Authors = Seq(
    Author(0, "Martin Odersky"),
    Author(1, "Joshua Suereth"),
    Author(2, "Bill_Venners")
  )

  val Books = Seq(
    Book("Programming in Scala", Seq(0, 1, 2)),
    Book("Scala in Depth", Seq(1, 0)),
    Book("Sbt in Action", Seq(1, 3)),
    Book("Inside the Java 2 Virtual Machine", Seq(2)),
    Book("Thining in Java", Seq(4))
  )
}

class UnboundedJoinBoundedTest extends PipelineSpec {

  import UnboundedJoinBounded._
  import UnboundedJoinBoundedTest._

  "Bounded stream" should "be joined to unbounded using map side input" in {
    JobTest[UnboundedJoinBounded.type]
      .args(
        "--authorsTable=dataset.authors",
        "--booksSubscription=books-subscription",
        "--booksDetailsTopic=books-details-topic"
      )
      .input(BigQueryIO[Author]("dataset.authors"), Authors)
      .inputStream(
        PubsubIO[Book]("books-subscription"), testStreamOf[Book]
          .addElements(Books(0))
          .addElements(Books(1))
          .addElements(Books(2))
          .addElements(Books(3))
          .addElements(Books(4))
          .advanceWatermarkToInfinity()
      )
      .output(
        PubsubIO[BookDetails]("books-details-topic"))(
        _ should containInAnyOrder(Seq(
          BookDetails("Programming in Scala", Seq(Authors(0), Authors(1), Authors(2))),
          BookDetails("Scala in Depth", Seq(Authors(1), Authors(0))),
          BookDetails("Sbt in Action", Seq(Authors(1))),
          BookDetails("Inside the Java 2 Virtual Machine", Seq(Authors(2))),
          BookDetails("Thining in Java", Seq())
        ))
      )
      .counter(MissedAuthorsCounter)(_ shouldBe 2)
      .run()
  }
}
