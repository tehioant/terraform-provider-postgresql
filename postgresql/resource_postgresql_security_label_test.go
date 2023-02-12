package postgresql

import (
	"database/sql"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"testing"
)

func TestAccPostgresqlSecurityLabel_Basic(t *testing.T) {
	var name = "skynet"
	config := `
resource "postgresql_security_label" "basic_security_label" {
	label_provider = "anon"
	type = "ROLE"
	name = "skynet"
    label = "'MASKED'"
	
}
`
	_, teardown := setupTestDatabase(t, false, true)
	defer teardown()

	createTestRole(t, name)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlSecurityLabelDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(
						"postgresql_security_label.basic_security_label", "label_provider", "anon"),
					resource.TestCheckResourceAttr(
						"postgresql_security_label.basic_security_label", "type", "ROLE"),
					resource.TestCheckResourceAttr(
						"postgresql_security_label.basic_security_label", "name", name),
					testAccCheckPostgresqlSecurityLabelExists(name),
				),
			},
		},
	})
}

func testAccCheckPostgresqlSecurityLabelExists(roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkSecurityLabelExists(client, roleName)
		if err != nil {
			return fmt.Errorf("Error checking security label %s", err)
		}

		if !exists {
			return fmt.Errorf("Security label not found")
		}

		return nil
	}
}

func checkSecurityLabelExists(client *Client, roleName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	err = db.QueryRow("SELECT provider, label, rolname"+
		" FROM pg_roles roles"+
		" INNER JOIN pg_shseclabel label"+
		" ON roles.oid = label.objoid"+
		" WHERE rolname=$1", roleName).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about security label: %s", err)
	}

	return true, nil
}

func testAccCheckPostgresqlSecurityLabelDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "postgresql_security_label" {
			continue
		}

		exists, err := checkSecurityLabelExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking security label %s", err)
		}

		if exists {
			return fmt.Errorf("Security label still exists after destroy")
		}
	}

	return nil
}