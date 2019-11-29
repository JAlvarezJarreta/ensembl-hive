=pod 

=head1 NAME

    Bio::EnsEMBL::Hive::DBSQL::BaseAdaptor

=head1 DESCRIPTION

    The base class for all other Object- or NakedTable- adaptors.
    Performs the low-level SQL needed to retrieve and store data in tables.

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2019] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=cut


package Bio::EnsEMBL::Hive::DBSQL::UniqueAdaptor;

use strict;
use warnings;

use Bio::EnsEMBL::Hive::Utils ('throw');

use base ('Bio::EnsEMBL::Hive::DBSQL::BaseAdaptor');

sub category_fields {
    throw("Please define category_fields by setting it via the category_fields() method your adaptor class");
}

sub checksum_field {
    throw("Please define checksum_field by setting it via the checksum_field() method your adaptor class");
}

sub representative_field {
    throw("Please define representative_field by setting it via the representative_field() method your adaptor class");
}


sub find_representative {
    my ($self, $categories, $checksum, $candidate) = @_;

    my $table_name              = $self->table_name;
    my $category_fields         = $self->category_fields;
    my $representative_field    = $self->representative_field;
    my $checksum_field          = $self->checksum_field;

    my $category_sql_part       = join(' AND ', map {"$_ = ?"} @$category_fields);
    my $exist_sql               = "SELECT $representative_field FROM $table_name WHERE $category_sql_part AND $checksum_field = ?";

    my $exist_result            = $self->dbc->protected_select(
        [ $exist_sql, @$categories, $checksum ],
        sub { my ($after) = @_; $self->db->get_LogMessageAdaptor->store_hive_message( "finding if there is an entry for $checksum in $table_name$after", 'INFO' ); }
    );

    if ($exist_result && scalar(@$exist_result)) {
        return $exist_result->[0]->{$representative_field};
    } else {
        return undef;
    }
}

 
sub find_or_register_representative {
    my ($self, $categories, $checksum, $candidate_representative_dbID) = @_;

    my $table_name              = $self->table_name;
    my $category_fields         = $self->category_fields;
    my $representative_field    = $self->representative_field;
    my $checksum_field          = $self->checksum_field;

    if (my $exist_result = $self->find_representative($categories, $checksum)) {
        return $exist_result;
    }

    my @all_fields = (@$category_fields, $checksum_field, $representative_field);
    my $insert_sql = "INSERT INTO $table_name (" . join(", ", @all_fields) . ") VALUES (" . join(",", ('?') x scalar(@all_fields)) . ")";

    eval {
        $self->dbc->protected_prepare_execute(
            [ $insert_sql, @$categories, $checksum, $candidate_representative_dbID ],
            sub { my ($after) = @_; $self->db->get_LogMessageAdaptor->store_hive_message( "inserting an entry for $checksum in $table_name$after", 'INFO' ); }
        );
        1;
    } or do {
        # The entry was freshly inserted by another process;
        my $duplicate_regex = {
            'mysql'     => qr/Duplicate entry.+?for key/s,
            'sqlite'    => qr/columns.+?are not unique|UNIQUE constraint failed/s,  # versions around 3.8 spit the first msg, versions around 3.15 - the second
            'pgsql'     => qr/duplicate key value violates unique constraint/s,
        }->{$self->db->dbc->driver};

        if( $@ =~ $duplicate_regex ) {      # implementing 'INSERT IGNORE' of Jobs on the API side

            my $exist_result = $self->find_representative($categories, $checksum);
            die "Cannot find an entry that is supposed to be there" unless $exist_result;
            return $exist_result;
        } else {
            die $@;
        }
    };
    return $candidate_representative_dbID;
}



1;

