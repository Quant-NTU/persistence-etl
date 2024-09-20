package sg.com.quantai.etl.data

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table

@Entity
@Table(name = "tests")
class Test{
    // Indexes
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private val id: Long = 0
    // Columns
    @Column(name = "firstName", nullable = false)
    private val firstName: String = ""
    @Column(name = "lastName", nullable = false)
    private val lastName: String = ""
}