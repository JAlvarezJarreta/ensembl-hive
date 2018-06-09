=pod

=head1 NAME

Bio::EnsEMBL::Hive::DBSQL::AnalysisJobAdaptor

=head1 SYNOPSIS

    $analysisJobAdaptor = $db_adaptor->get_AnalysisJobAdaptor;
    $analysisJobAdaptor = $analysisJob->adaptor;

=head1 DESCRIPTION

    Module to encapsulate all db access for persistent class AnalysisJob.
    There should be just one per application and database connection.

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2018] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=head1 APPENDIX

    The rest of the documentation details each of the object methods.
    Internal methods are preceded with a _

=cut


package Bio::EnsEMBL::Hive::DBSQL::UniqueTableAdaptor;

use strict;
use warnings;

use Digest::MD5 qw(md5_hex);

use Bio::EnsEMBL::Hive::Utils ('stringify', 'destringify');

use base ('Bio::EnsEMBL::Hive::DBSQL::NakedTableAdaptor');



sub check_uniqueness {
    my ($self, $checksum, $representative_job_id) = @_;

    my $table_name = $self->table_name;

    # Check if this is a rerun (better not to trust retry_count
    my $exist_sql = "SELECT 1 FROM $table_name WHERE representative_job_id = ? AND param_checksum = ?";
    my $exist_job = $self->dbc->protected_select( [ $exist_sql, $representative_job_id, $checksum ],
        sub { my ($after) = @_; $self->db->get_LogMessageAdaptor->store_hive_message( 'finding if the job has already run'.$after, 'INFO' ); }
    );
    if ($exist_job && scalar(@$exist_job)) {
        # reruns don't count
        return 0;
    }

    my $sql = 'INSERT INTO unique_job (analysis_id, param_checksum, representative_job_id) VALUES (?,?,?)';

    my $is_redundant = 0;

    eval {
        $self->dbc->protected_prepare_execute( [ $sql, $job->analysis_id, $checksum, $job->dbID ],
            sub { my ($after) = @_; $self->db->get_LogMessageAdaptor->store_hive_message( 'checking the job unicity'.$after, 'INFO' ); }
        );
        1;
    } or do {
        my $duplicate_regex = {
            'mysql'     => qr/Duplicate entry.+?for key/s,
            'sqlite'    => qr/columns.+?are not unique|UNIQUE constraint failed/s,  # versions around 3.8 spit the first msg, versions around 3.15 - the second
            'pgsql'     => qr/duplicate key value violates unique constraint/s,
        }->{$self->db->dbc->driver};

        if( $@ =~ $duplicate_regex ) {      # implementing 'INSERT IGNORE' of Jobs on the API side

            my $other_sql = 'SELECT representative_job_id FROM unique_job WHERE analysis_id = ? AND param_checksum = ?';
            my $other_job = $self->dbc->protected_select( [ $other_sql, $job->analysis_id, $checksum ],
                sub { my ($after) = @_; $self->db->get_LogMessageAdaptor->store_hive_message( 'finding the representative job'.$after, 'INFO' ); }
            );

            my $other_job_id    = $other_job->[0]->{'representative_job_id'};
            my $this_job_id     = $job->dbID;
            my $analysis_id     = $job->analysis_id;
            my $msg             = "Discarding this job because another job (job_id=$other_job_id) is already onto (analysis_id=$analysis_id, param_checksum=$checksum)";
            $self->db->get_LogMessageAdaptor->store_job_message( $this_job_id, $msg, 'INFO' );

            $is_redundant = 1;
        } else {
            die $@;
        }
    };

    return $is_redundant;
}

1;

