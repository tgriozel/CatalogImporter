package project.job

sealed trait JobError { val message: String }

case class JobConfigError(override val message: String) extends JobError

case class InputReadError(override val message: String) extends JobError

case class OutputWriteError(override val message: String) extends JobError
