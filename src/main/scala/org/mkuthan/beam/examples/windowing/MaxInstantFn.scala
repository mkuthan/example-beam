package org.mkuthan.beam.examples.windowing

import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn
import org.joda.time.Instant

class MaxInstantFn extends BinaryCombineFn[Instant] {

  override def apply(left: Instant, right: Instant): Instant =
    if (right.isAfter(left)) right else left

  override def identity(): Instant = Instant.EPOCH

}
