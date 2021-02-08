package DBIx::Class::ResultDDL;
# capture the default values of $^H and $^W for this version of Perl
BEGIN { $DBIx::Class::ResultDDL::_default_h= $^H; $DBIx::Class::ResultDDL::_default_w= $^W; }
use Exporter::Extensible -exporter_setup => 1;
use B::Hooks::EndOfScope 'on_scope_end';
use Carp;

# ABSTRACT: Sugar methods for declaring DBIx::Class::Result data definitions
# VERSION

=head1 SYNOPSIS

  package MyApp::Schema::Result::Artist;
  use DBIx::Class::ResultDDL qw/ -V1 -inflate_datetime -inflate_json /;
  
  table 'artist';
  col id           => integer unsigned auto_inc;
  col name         => varchar(25), null;
  col formed       => date;
  col disbanded    => date, null;
  col general_info => json null;
  col last_update  => datetime('UTC');
  primary_key 'id';
  
  idx artist_by_name => [ 'name' ];
  
  has_many albums => { id => 'Album.artist_id' };
  rel_many impersonators => { name => 'Artist.name' };

=head1 DESCRIPTION

This is Yet Another Sugar Module for building DBIC result classes.  It provides a
domain-specific-language that feels almost like writing DDL.

This module heavily pollutes your symbol table in the name of extreme convenience, so the
C<-Vx> option has the added feature of automatically removing those symbols at end-of-scope
as if you had said C<use namespace::clean;>.

This module has a versioned API, to help prevent name collisions.  If you request the C<-Vx>
behavior, you can rely on that to remain the same across upgrades.

=head1 EXPORTED FEATURES

This module is based on L<Exporter::Extensible>, allowing all the import notations that module
provides.  Every export beginning with a dash indicates custom behavior, rather than just a
simple export of symbols.

=head2 C<-swp>

"Strict, Warnings, Parent".

Enable C<use strict> and C<use warnings> unless those flags have been changed from the default
via other means.  In other words, you can still C<< use Moo >> or C<< use common::sense >>
without this option overriding your choice.

Then, C<< use parent "DBIx::Class::Core" >> unless the class already has an C<add_column>
method.  If C<add_column> exists it is presumably because you already declared a parent class.
Note that this check happens at BEGIN-time, so if you use Moo and C<< extends 'SomeClass'; >>
you need to wrap that in a begin block before the C<< use DBIx::Class::ResultDDL -V1 >> line.

=cut

our $CALLER; # can be used localized to wrap caller context into an anonymous sub

sub swp :Export(-) {
	my $self= shift;
	require strict; strict->import if $^H == $DBIx::Class::ResultDDL::_default_h;
	require warnings; warnings->import if $^W == $DBIx::Class::ResultDDL::_default_w;
	$self->_inherit_dbic;
}
sub _inherit_dbic {
	my $self= shift;
	my $pkg= $self->{into};
	unless ($pkg->can('load_components') && $pkg->can('add_column')) {
		require DBIx::Class::Core;
		no strict 'refs';
		push @{ $pkg . '::ISA' }, 'DBIx::Class::Core';
	}
}

=head2 C<-autoclean>

Remove all added symbols at the end of current scope.

=cut

sub autoclean :Export(-) {
	my $self= shift;
	my $sref= $self->exporter_config_scope;
	$self->exporter_config_scope($sref= \my $x) unless $sref;
	on_scope_end { $$sref->clean };
}

=head2 C<-V1>

Implies C<-swp>, C<:V1>, and C<-autoclean>.

=head2 C<-V0>

Implies C<-swp>, C<:V0>, and C<-autoclean>.

=cut

sub V1 :Export(-) {
	shift->exporter_also_import('-swp',':V1','-autoclean');
}
sub exporter_autoload_symbol {
	my ($self, $sym)= @_;
	if ($sym =~ /^-V([0-9]+)$/) {
		my $tag= ":V$1";
		my $method= sub { shift->exporter_also_import('-swp',$tag,'-autoclean') };
		return $self->exporter_register_option("V$1", $method);
	}
	return shift->next::method(@_);
}

# The functions and tag list for previous versions are not loaded by default.
# They are contained in a separate package ::V$N, which inherits many methods
# from this one but then overrides all the ones whose API were different in
# the past version.
# In order to make those versions exportable, they have to be loaded into
# the cache or symbol table of this package before they can be added to a tag
# to get exported.  This also requires that they be given a different name
# The pattern used here is to prefix "v0_" and so on to the methods which
# are re-defined in the subclass.
sub exporter_autoload_tag {
	my ($self, $name)= @_;
	my $class= ref $self || $self;
	if ($name =~ /^V([0-9]+)$/) {
		my $v_pkg= "DBIx::Class::ResultDDL::$name";
		my $v= $1;
		eval "require $v_pkg"
			or croak "Can't load package $v_pkg: $@";
		my $ver_exports= $v_pkg->exporter_get_tag($name);
		# For each tag member, see if it is the same as the method in this class.
		# If not, bring it in as v${X}_${name} and then export { -as => $name }
		my @tag;
		for (@$ver_exports) {
			if ($class->can($_) == $v_pkg->can($_)) {
				push @tag, $_;
			}
			else {
				my $install_as= "v${v}_$_";
				$class->exporter_export($install_as => $v_pkg->can($_));
				push @tag, $install_as, { -as => $_ };
			}
		}
		return \@tag;
	}
	return shift->next::method(@_);
}

=head2 C<-inflate_datetime>

Inflate all date columns to DateTime objects, by adding the DBIC component
L<DBIx::Class::InflateColumn::DateTime>.

=head2 C<-inflate_json>

Causes all columns declared with C<json> or C<jsonb> sugar methods to also
declare C<inflate_json>.

=cut

sub enable_inflate_datetime :Export(-inflate_datetime) {
	my $self= shift;
	$self->_inherit_dbic;
	$self->{into}->load_components('InflateColumn::DateTime')
		unless $self->{into}->isa('DBIx::Class::InflateColumn::DateTime');
}

sub enable_inflate_json :Export(-inflate_json) {
	my $self= shift;
	$self->_inherit_dbic;
	$self->{into}->load_components('InflateColumn::Serializer')
		unless $self->{into}->isa('DBIx::Class::InflateColumn::Serializer');
	($self->{ResultDDL_json_defaults} ||= {})->{serializer_class}= 'JSON';
}

=head1 EXPORTED COLLECTIONS

=head2 C<:V1>

This tag selects the following symbols:

  table view
  col
    null default auto_inc fk
    integer unsigned tinyint smallint bigint decimal numeric money
    float float4 float8 double real
    char varchar nchar nvarchar binary varbinary blob text ntext uuid
    date datetime timestamp enum bool boolean
    json jsonb inflate_json array
  primary_key idx create_index unique sqlt_add_index sqlt_add_constraint
  rel_one rel_many has_one might_have has_many belongs_to many_to_many
    ddl_cascade dbic_cascade

=cut

my @V1= qw(
	table view
	col
	  null default auto_inc fk
	  integer unsigned tinyint smallint bigint decimal numeric money
	  float float4 float8 double real
	  char varchar nchar nvarchar binary varbinary blob text ntext uuid
	  date datetime timestamp enum bool boolean
	  json jsonb inflate_json array
	primary_key idx create_index unique sqlt_add_index sqlt_add_constraint
	rel_one rel_many has_one might_have has_many belongs_to many_to_many
	  ddl_cascade dbic_cascade
);
our %EXPORT_TAGS;
$EXPORT_TAGS{V1}= \@V1;
export grep !/^jsonb?$/, @V1; # the json and jsonb functions are exported as generators, below

=head2 C<:V0>

See L<DBIx::Class::ResultDDL::V0>.  The primary difference from V1 is lack of array
column support, lack of index declaration support, and sugar methods do not pass
through leftover unknown arguments.  Also new Postgres column types were added in V1.

=head1 EXPORTED FUNCTIONS

=head2 table

  table 'foo';
  # becomes...
  __PACKAGE__->table('foo');

=cut

sub table {
	my $name= shift;
	DBIx::Class::Core->can('table')->(scalar($CALLER||caller), $name);
}

=head2 col

  col $name, @options;
  # becomes...
  __PACKAGE__->add_column($name, { is_nullable => 0, @merged_options });

Define a column.  This calls add_column after sensibly merging all your options.
It defaults the column to not-null for you, but you can override that by saying
C<null> in your options.
You will probably use many of the methods below to build the options for the column:

=cut

sub col {
	my $name= shift;
	croak "Odd number of arguments for col(): (".join(',',@_).")"
		if scalar(@_) & 1;
	my $opts= { is_nullable => 0 };
	# Apply options to the hash in order, so that they get overwritten as expected
	while (@_) {
		my ($k, $v)= (shift, shift);
		$opts->{$k}= $v, next
			unless index($k, '.') >= 0;
		# We support "foo.bar => $v" syntax which we convert to "foo => { bar => $v }"
		# because "foo => { bar => 1 }, foo => { baz => 2 }" would overwrite eachother.
		my @path= split /\./, $k;
		$k= pop @path;
		my $dest= $opts;
		$dest= ($dest->{$_} ||= {}) for @path;
		$dest->{$k}= $v;
	}
	my $pkg= $CALLER || caller;
	$pkg->add_column($name, $opts);
	1;
}

sub _maybe_array {
	my @dims;
	while (@_ && ref $_[0] eq 'ARRAY') {
		my $array= shift @_;
		push @dims, @$array? @$array : '';
	}
	join '', map "[$_]", @dims
}
sub _maybe_size {
	return shift if @_ && Scalar::Util::looks_like_number($_[0]);
	return undef;
}
sub _maybe_size_or_max {
	return shift if @_ && (Scalar::Util::looks_like_number($_[0]) || uc($_[0]) eq 'MAX');
	return undef;
}
sub _maybe_timezone {
	# This is a weak check, but assume the timezone will have at least one capital letter,
	# and that DBIC column attribute names will not.
	return shift if @_ && !ref $_[0] && $_[0] =~ /(^floating$|^local$|[A-Z])/;
	return undef;
}

=over

=item C<null>

  is_nullable => 1

=item C<auto_inc>

  is_auto_increment => 1, 'extra.auto_increment_type' => 'monotonic'

(The 'monotonic' bit is required to correctly deploy on SQLite.  You can read the
L<gory details|https://github.com/dbsrgits/sql-translator/pull/26> but the short
version is that SQLite gives you "fake" autoincrement by default, and you only get
real ANSI-style autoincrement if you ask for it.  SQL::Translator doesn't ask for
the extra work by default, but if you're declaring columns by hand expecting it to
be platform-neutral, then you probably want this.  SQLite also requires data_type
"integer", and for it to be the primary key.)

=item C<fk>

  is_foreign_key => 1

=item C<< default($value | @value) >>

  default_value => $value
  default_value => [ @value ] # if more than one param

=cut

sub null        { is_nullable => 1, @_ }
sub auto_inc    { is_auto_increment => 1, 'extra.auto_increment_type' => 'monotonic', @_ }
sub fk          { is_foreign_key => 1, @_ }
sub default     { default_value => (@_ > 1? [ @_ ] : $_[0]) }

=item C<integer>, C<integer($size)>, C<integer[]>, C<integer $size,[]>

  data_type => 'integer', size => $size || 11
  data_type => 'integer[]', size => $size || 11

=item C<unsigned>

  extra => { unsigned => 1 }

MySQL specific flag which can be combined with C<integer>

=item C<tinyint>

  data_type => 'tinyint', size => 4

=item C<smallint>

  data_type => 'smallint', size => 6

=item C<bigint>

  data_type => 'bigint', size => 22

=item C<numeric>, C<numeric($p)>, C<numeric($p,$s)>, C<numeric[]>, C<numeric $p,$s,[]>

  data_type => 'numeric'
  data_type => 'numeric', size => [ $p ]
  data_type => 'numeric', size => [ $p, $s ]
  data_type => 'numeric[]'
  data_type => 'numeric[]', size => [ $p, $s ]

=item C<decimal>

Identical to C<numeric>, but sets C<< data_type => 'decimal' >>

=item C<money>, C<money[]>

  data_type => 'money'
  data_type => 'money[]'

=item C<real>, C<real[]>

  data_type => 'real'
  data_type => 'real[]'

=item C<float4>, C<float4[]>

  data_type => 'float4'
  data_type => 'float4[]'

=item C<double>, C<double[]>

  data_type => 'double precision'
  data_type => 'double precision[]'

=item C<float8>, C<float8[]>

  data_type => 'float8'
  data_type => 'float8[]'

=item C<float>, C<float($bits)>, C<float[]>, C<float $bits,[]>

  data_type => 'float'
  data_type => 'float', size => $bits
  data_type => 'float[]'
  data_type => 'float[], size => $bits

SQLServer and Postgres offer this, where C<$bits> is the number of bits of precision
of the mantissa.  Array notation is supported for Postgres.

=cut

sub integer     {
	my $size= shift if @_ && Scalar::Util::looks_like_number($_[0]);
	data_type => 'integer'.&_maybe_array, size => $size || 11, @_
}
sub unsigned    { 'extra.unsigned' => 1, @_ }
sub tinyint     { data_type => 'tinyint',   size =>  4, @_ }
sub smallint    { data_type => 'smallint',  size =>  6, @_ }
sub bigint      { data_type => 'bigint',    size => 22, @_ }
sub decimal     { _numeric(decimal => @_) }
sub numeric     { _numeric(numeric => @_) }
sub _numeric    {
	my $type= shift;
	my $precision= &_maybe_size;
	my $size;
	if (defined $precision) {
		my $scale= &_maybe_size;
		$size= defined $scale? [ $precision, $scale ] : [ $precision ];
	}
	return data_type => $type.&_maybe_array, ($size? ( size => $size ) : ()), @_;
}
sub money       { data_type => 'money'.&_maybe_array, @_ }
sub double      { data_type => 'double precision'.&_maybe_array, @_ }
sub float8      { data_type => 'float8'.&_maybe_array, @_ }
sub real        { data_type => 'real'.&_maybe_array, @_ }
sub float4      { data_type => 'float4'.&_maybe_array, @_ }
# the float used by SQL Server allows variable size spec as number of bits of mantissa
sub float       { my $size= &_maybe_size; data_type => 'float'.&_maybe_array, (defined $size? (size => $size) : ()), @_ }

=item C<char>, C<char($size)>, C<char[]>, C<char $size,[]>

  data_type => 'char', size => $size // 1
  data_type => 'char[]', size => $size // 1

=item C<varchar>, C<varchar($size)>, C<varchar(MAX)>, C<varchar[]>, C<varchar $size,[]>

  data_type => 'varchar'
  data_type => 'varchar', size => $size  # "MAX" is a valid size for SQL Server
  data_type => 'varchar[]'
  data_type => 'varchar[]', size => $size

=item C<nchar>, C<nchar($size)>

SQL Server specific type for unicode char.  Same API as C<char>.

=item C<nvarchar>

SQL Server specific type for unicode varying character string.  Same API as C<varchar>.

=item C<MAX>

Constant for 'MAX', used by SQL Server for C<< varchar(MAX) >>.

=item C<binary>, C<binary($size)>, C<binary[]>, C<binary $size,[]>

  data_type => 'binary'
  data_type => 'binary', size => $size
  data_type => 'binary[]'
  data_type => 'binary[]', size => $size

=item C<varbinary>, C<varbinary($size)>, C<varbinary[]>, C<varbinary $size,[]>

  data_type => 'varbinary'
  data_type => 'varbinary', size => $size
  data_type => 'varbinary[]'
  data_type => 'varbinary[]', size => $size

=item C<bit>, C<bit($size)>, C<bit[]>, C<bit $size,[]>

  data_type => 'bit', size => $size // 1
  data_type => 'bit[]', size => $size // 1

Note that Postgres allows length-N bit strings, and arrays of length-N bit strings,
but SQL Server uses this same type name to represent a single bit.

=item C<varbit>, C<varbit($size)>, C<varbit[]>, C<varbit $size,[]>

  data_type => 'varbit'
  data_type => 'varbit', size => $size
  data_type => 'varbit[]'
  data_type => 'varbit[]', size => $size

=item C<blob>, C<blob($size)>

  data_type => 'blob',
  size => $size if defined $size

Note: For MySQL, you need to change the type according to '$size'.  A MySQL blob is C<< 2^16 >>
max length, and probably none of your binary data would be that small.  Consider C<mediumblob>
or C<longblob>, or consider overriding C<< My::Schema::sqlt_deploy_hook >> to perform this
conversion automatically according to which DBMS you are connected to.

For SQL Server, newer versions deprecate C<blob> in favor of C<VARCHAR(MAX)>.  This is another
detail you might take care of in sqlt_deploy_hook.

=item C<bytea>, C<bytea[]>

Postgres's blob type.  (no size is allowed)

=item C<tinyblob>

MySQL-specific type for small blobs

  data_type => 'tinyblob', size => 0xFF

=item C<mediumblob>

MySQL-specific type for larger blobs

  data_type => 'mediumblob', size => 0xFFFFFF

=item C<longblob>

MySQL-specific type for the longest supported blob type

  data_type => 'longblob', size => 0xFFFFFFFF

=item C<text>, C<text($size)>, C<text[]>

  data_type => 'text',
  data_type => 'text', size => $size
  data_type => 'text[]'

See MySQL notes in C<blob>.  For SQL Server, you might want C<ntext> or C<< varchar(MAX) >> instead.
Postgres does not use a size, and allows arrays of this type.

=item C<tinytext>

  data_type => 'tinytext', size => 0xFF

=item C<mediumtext>

  data_type => 'mediumtext', size => 0xFFFFFF

=item C<longtext>

  data_type => 'longtext', size => 0xFFFFFFFF

=item C<ntext>, C<ntext($size)>

SQL-Server specific type for unicode C<text>.  Note that newer versions prefer C<< nvarchar(MAX) >>.

  data_type => 'ntext', size => $size // 0x3FFFFFFF

=cut

sub char        { my $size= &_maybe_size;  data_type => 'char'.&_maybe_array, size => $size || 1, @_ }
sub nchar       { my $size= &_maybe_size;  data_type => 'nchar'.&_maybe_array, size => $size || 1, @_ }
sub varchar     { my $size= &_maybe_size_or_max;  data_type => 'varchar'.&_maybe_array, size => $size, @_ }
sub nvarchar    { my $size= &_maybe_size_or_max;  data_type => 'nvarchar'.&_maybe_array, size => $size, @_ }
sub binary      { my $size= &_maybe_size_or_max;  data_type => 'binary'.&_maybe_array, size => $size, @_ }
sub varbinary   { my $size= &_maybe_size_or_max;  data_type => 'varbinary'.&_maybe_array, size => $size, @_ }
sub bit         { my $size= &_maybe_size; data_type => 'bit'.&_maybe_array, size => (defined $size? $size : 1), @_ }
sub varbit      { my $size= &_maybe_size; data_type => 'varbit'.&_maybe_array, (defined $size? (size => $size) : ()), @_ }
sub MAX         { 'MAX' }

# postgres blob type
sub bytea       { data_type => 'bytea'.&_maybe_array, @_ }

# These aren't valid for Postgres, so no array notation needed
sub blob          { my $size= &_maybe_size;  data_type => 'blob', (defined $size? (size => $size) : ()), @_ }
sub tinyblob      { data_type => 'tinyblob',  size => 0xFF, @_ }
sub mediumblob    { data_type => 'mediumblob',size => 0xFFFFFF, @_ }
sub longblob      { data_type => 'longblob',  size => 0xFFFFFFFF, @_ }

sub text          { my $size= &_maybe_size_or_max;  data_type => 'text'.&_maybe_array, (defined $size? (size => $size) : ()), @_ }
sub ntext         { my $size= &_maybe_size_or_max;  data_type => 'ntext', size => ($size || 0x3FFFFFFF), @_ }
sub tinytext      { data_type => 'tinytext',  size => 0xFF, @_ }
sub mediumtext    { data_type => 'mediumtext',size => 0xFFFFFF, @_ }
sub longtext      { data_type => 'longtext',  size => 0xFFFFFFFF, @_ }

=item C<< enum( @values ) >>

  data_type => 'enum', extra => { list => [ @values ] }

=item C<bool>, C<boolean>

  data_type => 'boolean'

Note that SQL Server has 'bit' instead, though in postgres 'bit' is used for bitstrings.

=cut

sub enum        { data_type => 'enum', 'extra.list' => [ @_ ]}
sub boolean     { data_type => 'boolean'.&_maybe_array, @_ }
sub bool        { data_type => 'boolean'.&_maybe_array, @_ }

=item C<date>, C<date($timezone)>

  data_type => 'date'
  time_zone => $timezone if defined $timezone

=item C<datetime>, C<datetime($timezone)>

  data_type => 'datetime'
  time_zone => $timezone if defined $timezone

=item C<timestamp>, C<timestamp($timezone)>

  date_type => 'timestamp'
  time_zone => $timezone if defined $timezone

=cut

sub date        { data_type => 'date'.&_maybe_array, @_ }
sub datetime    { my $tz= &_maybe_timezone; data_type => 'datetime'.&_maybe_array, ($tz? (time_zone => $tz) : ()), @_ }
sub timestamp   { my $tz= &_maybe_timezone; data_type => 'timestamp'.&_maybe_array,($tz? (time_zone => $tz) : ()), @_ }

=item C<array($type)>, C<array(..., data_type => $type, ...)>

Declares a postgres array type with notation C<< data_type => $type . '[]' >>.
You may use sugar functions as arguments.

=cut

sub array {
	# If one argument and the argument is a string, then it is a type name
	if (@_ == 1 && $_[0] && !ref $_[0]) {
		return data_type => $_[0] . '[]';
	}
	# Else, scan through argument list looking for data_type, and append [] to following item.
	my $data_type_idx;
	for (my $i= 0; $i < @_; $i++) {
		$data_type_idx= $i+1 if $_[$i] eq 'data_type'
	}
	$data_type_idx && $_[$data_type_idx] && !ref $_[$data_type_idx]
		or die 'array needs a type';
	$_[$data_type_idx] .= '[]';
	return @_;
}

=item C<uuid>, C<uuid[]>

  data_type => 'uuid'
  data_type => 'uuid[]'

=cut

sub uuid          { data_type => 'uuid'.&_maybe_array, @_ }

=item C<json>, C<json[]>

  data_type => 'json'
  data_type => 'json[]'

If C<< -inflate_json >> use-line option was given, this will additionally imply C<< serializer_class => 'JSON' >>.

=item C<jsonb>, C<jsonb[]>

  data_type => 'jsonb'
  data_type => 'jsonb[]'

If C<< -inflate_json >> use-line option was given, this will additionally imply C<< serializer_class => 'JSON' >>.

=item C<inflate_json>

  serializer_class => 'JSON'

Also adds the component 'InflateColumn::Serializer' to the current package if it wasn't
added already.

=cut

# This is a generator that includes the json_args into the installed method.
sub json :Export(=) {
	my $json_defaults= $_[0]{ResultDDL_json_defaults} ||= {};
	return sub { data_type => 'json'.&_maybe_array, %$json_defaults, @_ }
}
sub jsonb :Export(=) {
	my $json_defaults= $_[0]{ResultDDL_json_defaults} ||= {};
	return sub { data_type => 'jsonb'.&_maybe_array, %$json_defaults, @_ }
}

sub inflate_json {
	my $pkg= ($CALLER||caller);
	$pkg->load_components('InflateColumn::Serializer')
		unless $pkg->isa('DBIx::Class::InflateColumn::Serializer');
	return serializer_class => 'JSON', @_;
}

=back

=head2 primary_key

  primary_key(@cols)

Shortcut for __PACKAGE__->set_primary_key(@cols)

=cut

sub primary_key { ($CALLER||caller)->set_primary_key(@_); }

=head2 unique

  unique($name?, \@cols)

Shortucut for __PACKAGE__->add_unique_constraint($name? \@cols)

=cut 

sub unique { ($CALLER||caller)->add_unique_constraint(@_) }

=head2 belongs_to

  belongs_to $rel_name, $peer_class, $condition, @attr_list;
  belongs_to $rel_name, { colname => "$ResultClass.$colname" }, @attr_list;
  # becomes...
  __PACKAGE__->belongs_to($rel_name, $peer_class, $condition, { @attr_list });

Note that the normal DBIC belongs_to requires conditions to be of the form

  { "foreign.$their_col" => "self.$my_col" }

but all these sugar functions allow it to be written the other way around, and use a table
name in place of "foreign.".

=head2 might_have

  might_have $rel_name, $peer_class, $condition, @attr_list;
  might_have $rel_name, { colname => "$ResultClass.$colname" }, @attr_list;
  # becomes...
  __PACKAGE__->might_have($rel_name, $peer_class, $condition, { @attr_list });

=head2 has_one

  has_one $rel_name, $peer_class, $condition, @attr_list;
  has_one $rel_name, { colname => "$ResultClass.$colname" }, @attr_list;
  # becomes...
  __PACKAGE__->has_one($rel_name, $peer_class, $condition, { @attr_list });

=head2 has_many

  has_many $rel_name, $peer_class, $condition, @attr_list;
  has_many $rel_name, { colname => "$ResultClass.$colname" }, @attr_list;
  # becomes...
  __PACKAGE__->has_many($rel_name, $peer_class, $condition, { @attr_list });
  
=head2 many_to_many

  many_to_many $name => $rel_to_linktable, $rel_from_linktable;
  # becomes...
  __PACKAGE__->many_to_many(@_);

=head2 rel_one

Declares a single-record left-join relation B<without implying ownership>.
Note that the DBIC relations that do imply ownership like C<might_have> I<cause an implied
deletion of the related row> if you delete a row from this table that references it, even if
your schema did not have a cascading foreign key.  This DBIC feature is controlled by the
C<cascading_delete> option, and using this sugar function to set up the relation defaults that
feature to "off".

  rel_one $rel_name, $peer_class, $condition, @attr_list;
  rel_one $rel_name, { $mycol => "$ResultClass.$fcol", ... }, @attr_list;
  # becomes...
  __PACKAGE__->add_relationship(
    $rel_name, $peer_class, { "foreign.$fcol" => "self.$mycol" },
    {
      join_type => 'LEFT',
      accessor => 'single',
      cascade_copy => 0,
      cascade_delete => 0,
      is_depends_on => $is_f_pk, # auto-detected, unless specified
      ($is_f_pk? fk_columns => { $mycol => 1 } : ()),
      @attr_list
    }
  );

=head2 rel_many

  rel_many $name => { $my_col => "$class.$col", ... }, @options;

Same as L</rel_one>, but generates a one-to-many relation with a multi-accessor.

=cut

sub rel_one {
	_add_rel(scalar($CALLER||caller), 'rel_one', @_);
}
sub rel_many {
	_add_rel(scalar($CALLER||caller), 'rel_many', @_);
}
sub might_have {
	_add_rel(scalar($CALLER||caller), 'might_have', @_);
}
sub has_one {
	_add_rel(scalar($CALLER||caller), 'has_one', @_);
}
sub has_many {
	_add_rel(scalar($CALLER||caller), 'has_many', @_);
}
sub belongs_to {
	_add_rel(scalar($CALLER||caller), 'belongs_to', @_);
}
sub many_to_many {
	DBIx::Class::Core->can('many_to_many')->(scalar($CALLER||caller), @_);
}

sub _add_rel {
	my ($pkg, $reltype, $name, $maybe_colmap, @opts)= @_;
	my ($rel_pkg, $dbic_colmap)= ref $maybe_colmap eq 'HASH'? _translate_colmap($maybe_colmap, $pkg)
		: !ref $maybe_colmap? ( _interpret_pkg_name($maybe_colmap, $pkg), shift(@opts) )
		: croak "Unexpected arguments";
	
	if ($reltype eq 'rel_one' || $reltype eq 'rel_many') {
		# Are we referring to the foreign row's primary key?  DBIC load order might not have
		# gotten there yet, so take a guess that if it isn't a part of our primary key, then it
		# is a part of their primary key.
		my @pk= $pkg->primary_columns;
		my $is_f_key= !grep { defined $dbic_colmap->{$_} || defined $dbic_colmap->{"self.$_"} } @pk;
		
		$pkg->add_relationship(
			$name,
			$rel_pkg,
			$dbic_colmap,
			{
				accessor => ($reltype eq 'rel_one'? 'single' : 'multi'),
				join_type => 'LEFT',
				($is_f_key? (
					fk_columns => { map { do {(my $x= $_) =~ s/^self\.//; $x } => 1 } values %$dbic_colmap },
					is_depends_on => 1,
					is_foreign_key_constraint => 1,
					undef_on_null_fk => 1,
				) : (
					is_depends_on => 0,
				)),
				cascade_copy => 0, cascade_delete => 0,
				@opts
			}
		);
	} else {
		require DBIx::Class::Core;
		DBIx::Class::Core->can($reltype)->($pkg, $name, $rel_pkg, $dbic_colmap, { @opts });
	}
}

sub _interpret_pkg_name {
	my ($rel_class, $current_pkg)= @_;
	# Related class may be relative to same namespace as current
	return $rel_class if index($rel_class, '::') >= 0;
	my ($parent_namespace)= ($current_pkg =~ /(.*)::[^:]+$/);
	return $parent_namespace.'::'.$rel_class;
}

# DBIC is normally { foreign.col => self.col } but I don't think that's very intuitive,
# so allow an alternate notation of { self_col => CLASS.col } and automatically determine
# which the user is using.
sub _translate_colmap {
	my ($colmap, $self_pkg)= @_;
	my ($rel_class, $direction, %result, $inconsistent)= ('',0);
	# First pass, find the values for $rel_class and $reverse
	for (keys %$colmap) {
		my ($key, $val)= ($_, $colmap->{$_});
		if ($key =~ /([^.]+)\.(.*)/) {
			if ($1 eq 'self') {
				$direction ||= 1;
				++$inconsistent if $direction < 0;
			}
			else {
				$direction ||= -1;
				++$inconsistent if $direction > 0;
				if ($1 ne 'foreign') {
					$rel_class ||= $1;
					++$inconsistent if $rel_class ne $1;
				}
			}
		}
		if ($val =~ /([^.]+)\.(.*)/) {
			if ($1 eq 'self') {
				$direction ||= -1;
				++$inconsistent if $direction > 0;
			}
			else {
				$direction ||= 1;
				++$inconsistent if $direction < 0;
				if ($1 ne 'foreign') {
					$rel_class ||= $1;
					++$inconsistent if $rel_class ne $1;
				}
			}
		}
	}
	croak "Inconsistent {self=>foreign} notation found in relation mapping"
		if $inconsistent;
	croak "Must reference foreign Result class name in one of the keys or values of relation mapping"
		unless $rel_class && $direction;
	# Related class may be relative to same namespace as current
	$rel_class= _interpret_pkg_name($rel_class, $self_pkg);
	
	# Second pass, rename the keys & values to DBIC canonical notation
	for (keys %$colmap) {
		my ($key, $val)= ($_, $colmap->{$_});
		$key =~ s/.*\.//;
		$val =~ s/.*\.//;
		$result{ $direction > 0? "foreign.$val" : "foreign.$key" }= $direction > 0? "self.$key" : "self.$val";
	}
	return $rel_class, \%result;
}

=head2 ddl_cascade

  ddl_cascade;     # same as ddl_cascade("CASCADE");
  ddl_cascade(1);  # same as ddl_cascade("CASCADE");
  ddl_cascade(0);  # same as ddl_cascade("RESTRICT");
  ddl_cascade($mode);

Helper method to generate C<@options> for above.  It generates

  on_update => $mode, on_delete => $mode

This does not affect client-side cascade, and is only used by Schema::Loader to generate DDL
for the foreign keys when the table is deployed.

=cut

sub ddl_cascade {
	my $mode= shift;
	$mode= 'CASCADE' if !defined $mode || $mode eq '1';
	$mode= 'RESTRICT' if $mode eq '0';
	return
		on_update => $mode,
		on_delete => $mode;
}

=head2 dbic_cascade

  dbic_cascade;  # same as dbic_cascade(1)
  dbic_cascade($enabled);

Helper method to generate C<@options> for above.  It generates

  cascade_copy => $enabled, cascade_delete => $enabled

This re-enables the dbic-side cascading that was disabled by default in the C<rel_> functions.

=cut

sub dbic_cascade {
	my $mode= defined $_[0]? $_[0] : 1;
	return
		cascade_copy => $mode,
		cascade_delete => $mode;
}

=head2 view

  view $view_name, $view_sql, %options;

Makes the current resultsource into a view. This is used instead of
'table'. Takes two options, 'is_virtual', to make this into a
virtual view, and  'depends' to list tables this view depends on.

Is the equivalent of

  __PACKAGE__->table_class('DBIx::Class::ResultSource::View');
  __PACKAGE__->table($view_name);

  __PACKAGE__->result_source_instance->view_definition($view_sql);
  __PACKAGE__->result_source_instance->deploy_depends_on($options{depends});
  __PACKAGE__->result_source_instance->is_virtual($options{is_virtual});

=cut
sub view {
        my ($name, $definition, %opts) = @_;
        my $pkg= $CALLER || caller;
        DBIx::Class::Core->can('table_class')->($pkg, 'DBIx::Class::ResultSource::View');
        DBIx::Class::Core->can('table')->($pkg, $name);

        my $rsi = $pkg->result_source_instance;
        $rsi->view_definition($definition);

        $rsi->deploy_depends_on($opts{depends}) if $opts{depends};
        $rsi->is_virtual($opts{virtual});
        
        return $rsi
}


=head1 INDEXES AND CONSTRAINTS

DBIx::Class doesn't actually track the indexes or constraints on a table.  If you want to add
these to be automatically deployed with your schema, you need an C<sqlt_deploy_hook> function.
This module can create one for you, but does not yet attempt to wrap one that you provide.
(You can of course wrap the one generated by this module using a method modifier from
L<Class::Method::Modifiers>)
The method C<sqlt_deploy_hook> is created in the current package the first time one of these
functions are called.  If it already exists and wasn't created by DBIx::Class::ResultDDL, it
will throw an exception.  The generated method does call C<maybe::next::method> for you.

=head2 sqlt_add_index

This is a direct passthrough to the function L<SQL::Translator::Schema::Table/add_index>,
without any magic.

See notes above about the generated C<sqlt_deploy_hook>.

=head2 sqlt_add_constraint

This is a direct passthrough to the function L<SQL::Translator::Schema::Table/add_constraint>,
without any magic.

See notes above about the generated C<sqlt_deploy_hook>.

=head2 create_index

  create_index $index_name => \@fields, %options;

This is sugar for sqlt_add_index.  It translates to

  sqlt_add_index( name => $index_name, fields => \@fields, options => \%options, (type => ?) );

where the C<%options> are the L<SQL::Translator::Schema::Index/options>, except if
one of the keys is C<type>, then that key/value gets pulled out and used as
L<SQL::Translator::Schema::Index/type>.

=head2 idx

Alias for L</create_index>; lines up nicely with 'col'.

=cut

our %_installed_sqlt_hook_functions;
sub _get_sqlt_hook_method_array {
	my $pkg= shift;
	$_installed_sqlt_hook_functions{$pkg} ||= do {
		# $pkg->can("sqlt_deploy_hook") is insufficient, because it might be declared
		# in a parent class, and that is not an error.  It is only an error if it was
		# already declared in this package.
		no strict 'refs';
		my $stash= %{$pkg.'::'};
		croak "${pkg}::sqlt_deploy_hook already exists; DBIx::Class::ResultDDL won't overwrite it."
			." (but you can use Moo(se) or Class::Method::Modifiers to apply your own wrapper to this generated method)"
			if $stash->{sqlt_deploy_hook} && $stash->{sqlt_deploy_hook}{CODE};

		# Create the sub once, bound to this array.  The array can then be extended without
		# needing to re-declare the sub.
		no warnings 'closure';
		my @methods;
		eval 'sub '.$pkg.'::sqlt_deploy_hook {
			my $self= shift;
			$self->maybe::next::method(@_);
			for (@methods) {
				my ($m, @args)= @$_;
				$_[0]->$m(@args);
			}
		} 1' or die "failed to generate sqlt_deploy_hook: $@";
		\@methods;
	};
}
sub sqlt_add_index {
	my $pkg= $CALLER || caller;
	my $methods= _get_sqlt_hook_method_array($pkg);
	push @$methods, [ add_index => @_ ];
}

sub sqlt_add_constraint {
	my $pkg= $CALLER || caller;
	my $methods= _get_sqlt_hook_method_array($pkg);
	push @$methods, [ add_constraint => @_ ];
}

sub create_index {
	my $pkg= $CALLER || caller;
	my $name= ref $_[0]? undef : shift;
	my $fields= shift;
	ref $fields eq 'ARRAY'
		or croak((defined $name? 'Second':'First').' argument must be arrayref of index fields');
	my %options= @_;
	my $type= delete $options{type};  # this is an attribute of Index, not a member of %options
	my $methods= _get_sqlt_hook_method_array($pkg);
	push @$methods, [
		add_index =>
			(defined $name? (name => $name) : ()),
			fields => $fields,
			(keys %options? (options => \%options) : ()),
			(defined $type? (type => $type) : ())
	];
}

BEGIN { *idx= *create_index; }

=head1 MISSING FUNCTIONALITY

The methods above in most cases allow you to insert plain-old-DBIC notation
where appropriate, instead of relying purely on sugar methods.
If you are missing your favorite column flag or something, feel free to
contribute a patch.

=head1 THANKS

Thanks to Clippard Instrument Laboratory Inc. and Ellis Partners in Management Solutions
for supporting open source, including portions of this module.

=cut

1;
