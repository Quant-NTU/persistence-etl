package sg.com.quantai.etl.repositories

import sg.com.quantai.etl.data.Test

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface TestRepository : JpaRepository<Test, Long> {
    fun findByFirstName(firstName: String): List<Test>
    fun findByLastName(lastName: String): List<Test>
}
