use Test2::V0;
use File::Temp;

BEGIN {
	# Test can't run without these, but this dist does not depend on them.
	for (qw( DBIx::Class::Schema::Loader DBIx::Class::Schema::Loader::DBI::SQLite DBD::SQLite )) {
		plan(skip_all => "Missing optional dependency $_")
			unless eval "require $_";
	}
}

{ # Example of subclassing the DBI::SQLite schema loader
	package MyLoader;
	use parent
		'DBIx::Class::ResultDDL::SchemaLoaderMixin',
		'DBIx::Class::Schema::Loader::DBI::SQLite';
	1;
}

# Create a temp dir for writing the SQLite database and dumping the schema
my $tmpdir= File::Temp->newdir;
my $dsn= "dbi:SQLite:$tmpdir/db.sqlite";
mkdir "$tmpdir/lib" or die "mkdir: $!";

# Populate the SQLite with a schema
my $db= DBI->connect($dsn, undef, undef, { AutoCommit => 1, RaiseError => 1 });
$db->do(<<SQL);
CREATE TABLE example (
	id integer primary key autoincrement not null,
	textcol text not null,
	varcharcol varchar(100),
	datetimecol datetime not null default CURRENT_TIMESTAMP
);
SQL
undef $db;

# Run Schema Loader on the SQLite database
DBIx::Class::Schema::Loader::make_schema_at(
	'My::Schema',
	{ debug => 1, dump_directory => "$tmpdir/lib" },
	[ $dsn, '', '', { loader_class => 'MyLoader' } ],
);

# Load the generated classes and verify the data that they declare
unshift @INC, "$tmpdir/lib";
ok( (eval 'require My::Schema' || diag $@), 'Able to load generated schema' );
is( [ My::Schema->sources ], [ 'Example' ], 'ResultSource list' );
is( [ My::Schema->source('Example')->columns ], [qw( id textcol varcharcol datetimecol )], 'Example column list' );

# Verify the sugar methods got used in the source code
my $example_src= slurp("$tmpdir/lib/My/Schema/Result/Example.pm");
verify_contains_lines( $example_src, <<'PL', 'Result::Example.pm' ) or diag "Unexpected sourcecode:\n$example_src";
use DBIx::Class::ResultDDL qw/ -V2 -inflate_datetime /;
table 'example';
col id          => integer, is_auto_increment => 1;
col textcol     => text;
col varcharcol  => varchar(100), null;
col datetimecol => datetime default(\'current_timestamp');
primary_key 'id';
PL

done_testing;


sub slurp { open my $fh, '<', $_[0] or die "open:$!"; local $/= undef; <$fh> }

# Run a subtest that ensures each line of $lines is found in-order in $text,
# ignoring whitespace differences and ignoring arbitrary lines inbetween.
sub verify_contains_lines {
	my ($text, $lines, $message)= @_;
	subtest $message => sub {
		pos($text)= 0;
		for (split /\n/, $lines) {
			my $regex= quotemeta($_).'\\ ';
			# replace run of escaped literal whitespace with whitespace wildcard
			$regex =~ s/(\\\s)+/\\s*/g;
			my $p= pos($text);
			unless ( ok( $text =~ /^$regex/mgc, "Found line '$_'" ) ) {
				note "Searching from: ".($text =~ /(.*)/gc? "'$1'" : '(end of input)');
				pos($text)= $p;
			}
		}
	};
}
