package DBIx::Class::ResultDDL::SchemaLoaderMixin;
use strict;
use warnings;
use List::Util 'max', 'all';
use DBIx::Class::ResultDDL;
use Data::Dumper ();
sub deparse { Data::Dumper->new([$_[0]])->Terse(1)->Quotekeys(0)->Sortkeys(1)->Indent(0)->Dump }

=head1 SYNOPSIS

  package MyLoader;
  use parent
	'DBIx::Class::ResultDDL::SchemaLoaderMixin', # mixin first
	'DBIx::Class::Schema::Loader::DBI::mysql';
  1;

  use DBIx::Class::Schema::Loader qw/ make_schema_at /;
  my %options= ...;
  my @conn_info= (
    'dbi:mysql:my_database',
    $user, $pass,
    { loader_class => 'MyLoader' }
  );
  make_schema_at($package, \%options, \@conn_info);

=head1 DESCRIPTION

This module overrides behavior of L<DBIx::Class::Schema::Loader::Base> to
generate Result files that use L<DBIx::Class::ResultDDL> notation.
C<< ::Schema::Loader::Base >> is the base class for all of the actual loader
classes, which are invoked by C<< ::Schema::Loader >> (but do not share a
class hierarchy).

This is essentially a Role, but Schema Loader isn't based on Moo(se) and this
ResultDDL distribution does not yet depend on Moo(se), so it just uses plain
perl multiple inheritance.  Inherit from the mixin first so that its methods
take priority.  (it does override private methods of schema loader, so without
the Role mechanism to verify it, there is a chance parts just stop working if
Schema Loader changes its internals.  But it's a development-time tool, and
you'll see the output change, and the output will still be valid)

=cut

#sub _write_classfile {
#   my ($self, $class, $text, $is_schema)= @_;
#   main::explain($class);
#   main::explain($text);
#   main::explain($self->{_dump_storage}{$class});
#   $self->next::method($class, $text, $is_schema);
#}

sub _dbic_stmt {
	my ($self, $class, $method)= splice(@_, 0, 3);
	$self->{_MyLoader_use_resultddl}{$class}++
		or $self->_raw_stmt($class, qq|use DBIx::Class::ResultDDL qw/ -V2 -inflate_datetime /;\n|);
	if ($method eq 'table') {
		$self->_raw_stmt($class, q|table |.deparse(@_).';');
	}
	elsif ($method eq 'add_columns') {
		my @col_defs;
		while (@_) {
			my ($col_name, $col_info)= splice(@_, 0, 2);
			push @col_defs, [ _maybe_quote_identifier($col_name), $self->generate_column_info_sugar($col_info) ];
		}
		# align the definitions
		my $widest= max map length($_->[0]), @col_defs;
		$self->_raw_stmt($class, sprintf("col %-*s => %s;", $widest, @$_))
			for @col_defs;
	}
	elsif ($method eq 'set_primary_key') {
		$self->_raw_stmt($class, q|primary_key |.deparse(@_).';');
	}
	else {
		$self->next::method($class, $method, @_);
	}
	return;
}

my %data_type_sugar= (
	(map {
		my $type= $_;
		$type => sub { my ($col_info)= @_;
			if ($col_info->{size} && $col_info->{size} =~ /^[0-9]+$/) {
				return "$type(".delete($col_info->{size})."),";
			} elsif ($col_info->{size} && ref $col_info->{size} eq 'ARRAY'
				&& (1 <= scalar($col_info->{size}->@*) <= 2)
				&& (all { /^[0-9]+$/ } $col_info->{size}->@*)
			) {
				return "$type(".join(',', delete($col_info->{size})->@*)."),";
			} else {
				return $type;
			}
		}
	} qw( integer float real numeric decimal varchar char )),
	(map {
		my $type= $_;
		$type => sub { my ($col_info)= @_;
			if ($col_info->{timezone} && !ref $col_info->{timezone}) {
				return "$type(".deparse(delete $col_info->{timezone})."),";
			} else {
				return $type;
			}
		}
	} qw( datetime timestamp )),
);

sub _get_data_type_sugar {
	my $col_info= shift;

	my $t= delete $col_info->{data_type}
		or return ();

	my $pl= ($data_type_sugar{$t} //= do {
		my $sugar= DBIx::Class::ResultDDL->can($t);
		my @out= $sugar? $sugar->() : ();
		@out >= 2 && $out[0] eq 'data_type' && $out[1] eq $t? sub { $t }
		: sub { 'data_type => '.deparse($t).',' }
	})->($col_info);

	if ($col_info->{extra} && $col_info->{extra}{unsigned}) {
		$pl =~ s/,?$/,/ unless $pl =~ /\w$/;
		$pl .= ' unsigned';
		if (1 == keys %{ $col_info->{extra} }) {
			delete $col_info->{extra};
		} else {
			$col_info->{extra}= { %{ $col_info->{extra} } };
			delete $col_info->{extra}{unsigned};
		}
	}
	return $pl;
}

sub _maybe_quote_identifier {
	# TODO: complete support for perl's left-hand of => operator parsing rule
	$_[0] =~ /^[A-Za-z0-9_]+$/? $_[0] : deparse($_[0]);
}

# Tke a hash of DBIx::Class column_info and return a string of the form
#  "$data_type($size), $null default($default_value), etc => ..."
# ajusting the string for max readability.
our $test_pkg_idx= 0;
sub generate_column_info_sugar {
	my ($self, $orig_col_info)= @_;

	my %col_info= %$orig_col_info;
	my $stmt= _get_data_type_sugar(\%col_info);
	$stmt .= ' null' if delete $col_info{is_nullable};
	$stmt .= ' default('.deparse(delete $col_info{default_value}).'),' if exists $col_info{default_value};

	# Test the syntax for equality to the original
	my %out;
	++$test_pkg_idx;
	eval "package DBIx::Class::ResultDDL_check$test_pkg_idx { use DBIx::Class::ResultDDL qw/ :V2 /; %out= (is_nullable => 0, $stmt); }";
	if ($out{'extra.unsigned'}) { $out{extra}{unsigned}= delete $out{'extra.unsigned'}; }

	# Ignore the problem where 'integer' generates a default size for mysql that wasn't
	# in the Schema Loader spec.  TODO: add an option to skip generating this.
	delete $out{size} if $out{size} && !$orig_col_info->{size};

	# Data::Dumper gets confused and thinks sizes need quoted
	if (defined $orig_col_info->{size} && $orig_col_info->{size} =~ /^[0-9]+$/) {
		$orig_col_info->{size}= 0 + $orig_col_info->{size};
	}
	
	if (deparse({ %col_info, %out }) eq deparse({ %$orig_col_info })) {
		# remove trailing comma
		$stmt =~ s/,\s*$//;
		# dump the rest, and done.
		$stmt .= ', '._maybe_quote_identifier($_).' => '.deparse($col_info{$_})
			for sort keys %col_info;
	}
	else {
		warn "Unable to use ResultDDL sugar '$stmt'\n  ".deparse({ %col_info, %out })." ne ".deparse($orig_col_info)."\n";
		$stmt= join(', ', map _maybe_quote_identifier($_).' => '.deparse($orig_col_info->{$_}), sort keys %$orig_col_info);
	}
	return $stmt;
}

1;
