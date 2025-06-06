% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataset.R
\name{open_dataset}
\alias{open_dataset}
\title{Open a multi-file dataset}
\usage{
open_dataset(
  sources,
  schema = NULL,
  partitioning = hive_partition(),
  hive_style = NA,
  unify_schemas = NULL,
  format = c("parquet", "arrow", "ipc", "feather", "csv", "tsv", "text", "json"),
  factory_options = list(),
  ...
)
}
\arguments{
\item{sources}{One of:
\itemize{
\item a string path or URI to a directory containing data files
\item a \link{FileSystem} that references a directory containing data files
(such as what is returned by \code{\link[=s3_bucket]{s3_bucket()}})
\item a string path or URI to a single file
\item a character vector of paths or URIs to individual data files
\item a list of \code{Dataset} objects as created by this function
\item a list of \code{DatasetFactory} objects as created by \code{\link[=dataset_factory]{dataset_factory()}}.
}

When \code{sources} is a vector of file URIs, they must all use the same protocol
and point to files located in the same file system and having the same
format.}

\item{schema}{\link{Schema} for the \code{Dataset}. If \code{NULL} (the default), the schema
will be inferred from the data sources.}

\item{partitioning}{When \code{sources} is a directory path/URI, one of:
\itemize{
\item a \code{Schema}, in which case the file paths relative to \code{sources} will be
parsed, and path segments will be matched with the schema fields.
\item a character vector that defines the field names corresponding to those
path segments (that is, you're providing the names that would correspond
to a \code{Schema} but the types will be autodetected)
\item a \code{Partitioning} or \code{PartitioningFactory}, such as returned
by \code{\link[=hive_partition]{hive_partition()}}
\item \code{NULL} for no partitioning
}

The default is to autodetect Hive-style partitions unless
\code{hive_style = FALSE}. See the "Partitioning" section for details.
When \code{sources} is not a directory path/URI, \code{partitioning} is ignored.}

\item{hive_style}{Logical: should \code{partitioning} be interpreted as
Hive-style? Default is \code{NA}, which means to inspect the file paths for
Hive-style partitioning and behave accordingly.}

\item{unify_schemas}{logical: should all data fragments (files, \code{Dataset}s)
be scanned in order to create a unified schema from them? If \code{FALSE}, only
the first fragment will be inspected for its schema. Use this fast path
when you know and trust that all fragments have an identical schema.
The default is \code{FALSE} when creating a dataset from a directory path/URI or
vector of file paths/URIs (because there may be many files and scanning may
be slow) but \code{TRUE} when \code{sources} is a list of \code{Dataset}s (because there
should be few \code{Dataset}s in the list and their \code{Schema}s are already in
memory).}

\item{format}{A \link{FileFormat} object, or a string identifier of the format of
the files in \code{x}. This argument is ignored when \code{sources} is a list of \code{Dataset} objects.
Currently supported values:
\itemize{
\item "parquet"
\item "ipc"/"arrow"/"feather", all aliases for each other; for Feather, note that
only version 2 files are supported
\item "csv"/"text", aliases for the same thing (because comma is the default
delimiter for text files
\item "tsv", equivalent to passing \verb{format = "text", delimiter = "\\t"}
\item "json", for JSON format datasets Note: only newline-delimited JSON (aka ND-JSON) datasets
are currently supported
Default is "parquet", unless a \code{delimiter} is also specified, in which case
it is assumed to be "text".
}}

\item{factory_options}{list of optional FileSystemFactoryOptions:
\itemize{
\item \code{partition_base_dir}: string path segment prefix to ignore when
discovering partition information with DirectoryPartitioning. Not
meaningful (ignored with a warning) for HivePartitioning, nor is it
valid when providing a vector of file paths.
\item \code{exclude_invalid_files}: logical: should files that are not valid data
files be excluded? Default is \code{FALSE} because checking all files up
front incurs I/O and thus will be slower, especially on remote
filesystems. If false and there are invalid files, there will be an
error at scan time. This is the only FileSystemFactoryOption that is
valid for both when providing a directory path in which to discover
files and when providing a vector of file paths.
\item \code{selector_ignore_prefixes}: character vector of file prefixes to ignore
when discovering files in a directory. If invalid files can be excluded
by a common filename prefix this way, you can avoid the I/O cost of
\code{exclude_invalid_files}. Not valid when providing a vector of file paths
(but if you're providing the file list, you can filter invalid files
yourself).
}}

\item{...}{additional arguments passed to \code{dataset_factory()} when \code{sources}
is a directory path/URI or vector of file paths/URIs, otherwise ignored.
These may include \code{format} to indicate the file format, or other
format-specific options (see \code{\link[=read_csv_arrow]{read_csv_arrow()}}, \code{\link[=read_parquet]{read_parquet()}} and \code{\link[=read_feather]{read_feather()}} on how to specify these).}
}
\value{
A \link{Dataset} R6 object. Use \code{dplyr} methods on it to query the data,
or call \code{\link[=Scanner]{$NewScan()}} to construct a query directly.
}
\description{
Arrow Datasets allow you to query against data that has been split across
multiple files. This sharding of data may indicate partitioning, which
can accelerate queries that only touch some partitions (files). Call
\code{open_dataset()} to point to a directory of data files and return a
\code{Dataset}, then use \code{dplyr} methods to query it.
}
\section{Partitioning}{


Data is often split into multiple files and nested in subdirectories based on the value of one or more
columns in the data. It may be a column that is commonly referenced in
queries, or it may be time-based, for some examples. Data that is divided
this way is "partitioned," and the values for those partitioning columns are
encoded into the file path segments.
These path segments are effectively virtual columns in the dataset, and
because their values are known prior to reading the files themselves, we can
greatly speed up filtered queries by skipping some files entirely.

Arrow supports reading partition information from file paths in two forms:
\itemize{
\item "Hive-style", deriving from the Apache Hive project and common to some
database systems. Partitions are encoded as "key=value" in path segments,
such as \code{"year=2019/month=1/file.parquet"}. While they may be awkward as
file names, they have the advantage of being self-describing.
\item "Directory" partitioning, which is Hive without the key names, like
\code{"2019/01/file.parquet"}. In order to use these, we need know at least
what names to give the virtual columns that come from the path segments.
}

The default behavior in \code{open_dataset()} is to inspect the file paths
contained in the provided directory, and if they look like Hive-style, parse
them as Hive. If your dataset has Hive-style partitioning in the file paths,
you do not need to provide anything in the \code{partitioning} argument to
\code{open_dataset()} to use them. If you do provide a character vector of
partition column names, they will be ignored if they match what is detected,
and if they don't match, you'll get an error. (If you want to rename
partition columns, do that using \code{select()} or \code{rename()} after opening the
dataset.). If you provide a \code{Schema} and the names match what is detected,
it will use the types defined by the Schema. In the example file path above,
you could provide a Schema to specify that "month" should be \code{int8()}
instead of the \code{int32()} it will be parsed as by default.

If your file paths do not appear to be Hive-style, or if you pass
\code{hive_style = FALSE}, the \code{partitioning} argument will be used to create
Directory partitioning. A character vector of names is required to create
partitions; you may instead provide a \code{Schema} to map those names to desired
column types, as described above. If neither are provided, no partitioning
information will be taken from the file paths.
}

\examples{
\dontshow{if (arrow_with_dataset() & arrow_with_parquet()) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
# Set up directory for examples
tf <- tempfile()
dir.create(tf)

write_dataset(mtcars, tf, partitioning = "cyl")

# You can specify a directory containing the files for your dataset and
# open_dataset will scan all files in your directory.
open_dataset(tf)

# You can also supply a vector of paths
open_dataset(c(file.path(tf, "cyl=4/part-0.parquet"), file.path(tf, "cyl=8/part-0.parquet")))

## You must specify the file format if using a format other than parquet.
tf2 <- tempfile()
dir.create(tf2)
write_dataset(mtcars, tf2, format = "ipc")
# This line will results in errors when you try to work with the data
\dontrun{
open_dataset(tf2)
}
# This line will work
open_dataset(tf2, format = "ipc")

## You can specify file partitioning to include it as a field in your dataset
# Create a temporary directory and write example dataset
tf3 <- tempfile()
dir.create(tf3)
write_dataset(airquality, tf3, partitioning = c("Month", "Day"), hive_style = FALSE)

# View files - you can see the partitioning means that files have been written
# to folders based on Month/Day values
tf3_files <- list.files(tf3, recursive = TRUE)

# With no partitioning specified, dataset contains all files but doesn't include
# directory names as field names
open_dataset(tf3)

# Now that partitioning has been specified, your dataset contains columns for Month and Day
open_dataset(tf3, partitioning = c("Month", "Day"))

# If you want to specify the data types for your fields, you can pass in a Schema
open_dataset(tf3, partitioning = schema(Month = int8(), Day = int8()))
\dontshow{\}) # examplesIf}
}
\seealso{
\href{https://arrow.apache.org/docs/r/articles/dataset.html}{
datasets article}
}
