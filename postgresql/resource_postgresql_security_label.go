package postgresql

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"strings"
)

const (
	labelProviderAttr = "label_provider"
	labelNameAttr     = "label"
	objectTypeAttr    = "type"
	objectNameAttr    = "name"
)

/* NOT SUPPORTED:
- [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] for objects of type FUNCTION, PROCEDURE, ROUTINE
*/

func resourcePostgreSQLSecurityLabel() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLSecurityLabelCreate),
		Read:   PGResourceFunc(resourcePostgreSQLSecurityLabelRead),
		Delete: PGResourceFunc(resourcePostgreSQLSecurityLabelDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLSecurityLabelExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			labelProviderAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "TTeh name of the provider",
				ForceNew:    true,
			},
			labelNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the object to be labeled (tables, functions, etc.) or NULL to drop the security label.",
				ForceNew:    true,
			},
			objectTypeAttr: {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			objectNameAttr: {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
		},
	}
}

func resourcePostgreSQLSecurityLabelCreate(db *DBConnection, d *schema.ResourceData) error {
	if err := createSecurityLabel(db, d); err != nil {
		return err
	}

	return resourcePostgreSQLSecurityLabelReadImpl(db, d)
}

func resourcePostgreSQLSecurityLabelRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLSecurityLabelReadImpl(db, d)
}

func resourcePostgreSQLSecurityLabelExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var rolName string
	err := db.QueryRow("SELECT provider, label, rolname"+
		" FROM pg_roles roles"+
		" INNER JOIN pg_shseclabel label"+
		" ON roles.oid = label.objoid"+
		" WHERE rolname=$1", d.Get(objectNameAttr)).Scan(&rolName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about security label: %s", err)
	}

	return true, nil
}

func resourcePostgreSQLSecurityLabelDelete(db *DBConnection, d *schema.ResourceData) error {
	if err := deleteSecurityLabel(db, d); err != nil {
		return err
	}
	return nil
}

func createSecurityLabel(db *DBConnection, d *schema.ResourceData) error {
	txn, _ := startTransaction(db.client, "")
	b := bytes.NewBufferString("SECURITY LABEL")

	fmt.Fprint(b, " FOR ", d.Get(labelProviderAttr).(string))
	fmt.Fprint(b, " ON ", d.Get(objectTypeAttr).(string), " ", d.Get(objectNameAttr).(string))
	fmt.Fprint(b, " IS ", d.Get(labelNameAttr).(string))

	secLabelSQL := b.String()
	if _, err := txn.Exec(secLabelSQL); err != nil {
		return fmt.Errorf("error creating security label on %s %s: %w", d.Get(objectTypeAttr).(string), d.Get(objectNameAttr).(string), err)
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func deleteSecurityLabel(db *DBConnection, d *schema.ResourceData) error {
	txn, _ := startTransaction(db.client, "")
	b := bytes.NewBufferString("SECURITY LABEL ")

	fmt.Fprint(b, "FOR ", d.Get(labelProviderAttr).(string))
	fmt.Fprint(b, "ON ", d.Get(objectTypeAttr).(string), d.Get(objectNameAttr).(string))
	fmt.Fprint(b, "IS ", "NULL")

	secLabelSQL := b.String()
	if _, err := txn.Exec(secLabelSQL); err != nil {
		return fmt.Errorf("error deleting security label on %s %s: %w", d.Get(objectTypeAttr).(string), d.Get(objectNameAttr).(string), err)
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func resourcePostgreSQLSecurityLabelReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var provider, label, name string
	labelID := d.Id()

	columns := []string{
		"provider",
		"label",
		"rolname",
	}
	values := []interface{}{
		provider,
		&label,
		&name,
	}

	secLabelSQL := fmt.Sprintf(`SELECT %s FROM pg_roles roles
		INNER JOIN pg_shseclabel label
		ON roles.oid = label.objoid
		WHERE rolname=$1`, strings.Join(columns, ", "),
	)

	err := db.QueryRow(secLabelSQL, labelID).Scan(values...)

	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL security label (%s) not found", labelID)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading security label: %w", err)
	}

	d.Set(labelProviderAttr, provider)
	d.Set(labelNameAttr, label)
	d.Set(objectTypeAttr, "ROLE")
	d.Set(objectNameAttr, name)
	d.SetId(name)

	return nil
}
