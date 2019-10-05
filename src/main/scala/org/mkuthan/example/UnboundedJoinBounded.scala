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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.ScioMetrics
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType

object UnboundedJoinBounded {

  type AuthorId = Int

  @BigQueryType.toTable
  case class Author(id: AuthorId, name: String)

  case class Book(title: String, authors: Seq[AuthorId])

  case class BookDetails(title: String, authors: Seq[Author])

  val MissedAuthorsCounter = ScioMetrics.counter("missedAuthors")
  val MissedAuthorCounterInc = () => {
    MissedAuthorsCounter.inc();
    None
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val authorsTable = args.required("authorsTable")
    val booksSubscription = args.required("booksSubscription")
    val booksDetailsTopic = args.required("booksDetailsTopic")

    val authors = sc.typedBigQuery[Author](authorsTable)
    val authorsSideInput = authors
      .map { author =>
        author.id -> author
      }
      .asMapSideInput

    val books = sc.pubsubSubscription[Book](booksSubscription)

    val booksDetails = books
      .withSideInputs(authorsSideInput)
      .map { (book, ctx) =>
        val authorsMap = ctx(authorsSideInput)

        val authors = for {
          authorId <- book.authors
          author <- authorsMap.get(authorId).orElse(MissedAuthorCounterInc())
        } yield author

        BookDetails(book.title, authors)
      }
      .toSCollection

    booksDetails.saveAsPubsub(booksDetailsTopic)

    sc.run()
    ()
  }

}
