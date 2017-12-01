=pod

=head1 NAME

    Bio::EnsEMBL::Hive::DBSQL::AttemptAdaptor

=head1 DESCRIPTION

    Module to encapsulate all db access for class Attempt.

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


package Bio::EnsEMBL::Hive::DBSQL::AttemptAdaptor;

use strict;
use warnings;

use Digest::MD5 qw(md5_hex);

use Bio::EnsEMBL::Hive::Attempt;
use Bio::EnsEMBL::Hive::Utils ('stringify');

use base ('Bio::EnsEMBL::Hive::DBSQL::ObjectAdaptor');

# ----------------------------- ObjectAdaptor implementation -----------------------------------

sub default_table_name {
    return 'attempt';
}


sub object_class {
    return 'Bio::EnsEMBL::Hive::Attempt';
}


# ------------------------------------ Attempt methods ------------------------------------------

sub check_job_uniqueness {
    my ($self, $attempt) = @_;

    my $job = $attempt->job;

    # Assumes the parameters have already been loaded
    my $checksum = md5_hex(stringify($job->{'_unsubstituted_param_hash'}));
    $attempt->param_checksum($checksum);
    $self->update_param_checksum($attempt);

    # Check if this is a rerun (better not to trust retry_count
    my $exist_sql = 'SELECT 1 FROM unique_job WHERE representative_job_id = ? AND param_checksum = ?';
    my $exist_job = $self->dbc->protected_select( [ $exist_sql, $job->dbID, $checksum ],
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

            die "Cannot check the unicity of the job" unless $other_job;

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



=head2 check_in_attempt

  Arg [1]    : Bio::EnsEMBL::Hive::Attempt $attempt
  Description: Update the status of an attempt and the when_updated timestamp.

=cut

sub check_in_attempt {
    my ($self, $attempt) = @_;

    my $attempt_id = $attempt->dbID;

    my $sql = "UPDATE attempt SET ";
      $sql .= "status='".$attempt->status."'";
      $sql .= ",when_updated=CURRENT_TIMESTAMP";
      $sql .= " WHERE attempt_id='$attempt_id' ";

    $self->dbc->do($sql);
}


=head2 record_attempt_completion

  Arg [1]    : Bio::EnsEMBL::Hive::Attempt $attempt
  Arg [2]    : Boolean $is_success: whether the attempt has successfully reached its end
  Description: Finalize the attempt entry. Mark the attempt as complete (whether
               successful or not) writing end time and statistics such as runtime_msec
               and query_count.

=cut

sub record_attempt_completion {
    my ($self, $attempt, $is_success) = @_;

    my $attempt_id = $attempt->dbID;

    my $sql = "UPDATE attempt SET ";
      $sql .= "status='END'";
      $sql .= ",when_updated=CURRENT_TIMESTAMP";
      $sql .= ",when_ended=CURRENT_TIMESTAMP";
      $sql .= ",is_success=$is_success";
      $sql .= ",runtime_msec=".($attempt->runtime_msec//'NULL');
      $sql .= ",query_count=".($attempt->query_count//'NULL');
      $sql .= " WHERE attempt_id='$attempt_id' ";

    $self->dbc->do($sql);
}


=head2 record_attempt_interruption

  Arg [1]    : Integer $attempt_id. Valid dbID of an attempt
  Description: Update the attempt table to mark this attempt as done but failed.

=cut

sub record_attempt_interruption {
    my ($self, $attempt_id) = @_;

    # Note that we don't update "when_updated" to keep a trace of the
    # last ping from the job
    my $sql = "UPDATE attempt SET ";
      $sql .= "status='END'";
      $sql .= ",when_ended=CURRENT_TIMESTAMP";
      $sql .= ",is_success=0";
      $sql .= " WHERE attempt_id='$attempt_id' ";

    $self->dbc->do($sql);
}


=head2 store_out_files

  Arg [1]    : Bio::EnsEMBL::Hive::Attempt $attempt
  Description: update locations of log files, if present
  Returntype : Boolean: whether the attempt has been updated in the database or not
  Exceptions : None
  Caller     : Bio::EnsEMBL::Hive::Worker

=cut

sub store_out_files {
    my ($self, $attempt) = @_;

    return $self->update_stdout_file_AND_stderr_file($attempt);
}


1;

