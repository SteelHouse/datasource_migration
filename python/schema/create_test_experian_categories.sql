create table test.experian_categories
(
    data_source_id          integer,
    data_source_category_id integer,
    experian_segment_id			varchar(65535),
    parent_id               integer,
    partner_id              integer,
    name                    varchar(65535),
    description             varchar(65535),
    path                    varchar(65535),
    names                   varchar(65535),
    path_from_root          varchar(65535),
    is_leaf_node            boolean,
    navigation_only         boolean,
    advertiser_id           integer,
    deprecated              boolean default false,
    public                  boolean,
    sort_order              integer,
    cpm											numeric(5,2),
    created_date            date    default CURRENT_DATE not null,
    updated_date            date
);
alter table test.experian_categories
    owner to awsuser;
grant delete, insert, select, update on test.experian_categories to group db_da;
grant delete, insert, select, update on test.experian_categories to group db_dae;
grant delete, insert, select, update on test.experian_categories to group db_ddm;
grant delete, insert, select, update on test.experian_categories to group db_tgt;
grant delete, insert, select, update on test.experian_categories to group db_per;
grant delete, insert, select, update on test.experian_categories to group db_dba;
grant delete, insert, select, update on test.experian_categories to group db_de;
grant delete, insert, select, update on test.experian_categories to group db_bae;
grant delete, insert, select, update on test.experian_categories to group db_dataload;
grant delete, insert, references, select, trigger, truncate, update on test.experian_categories to group db_ds;
grant select on test.experian_categories to group read_only;
grant select on test.experian_categories to group tableau_read_only;
grant delete, insert, select, update on test.experian_categories to group testuser;