package sg.com.quantai.etl.data

@Entity
@Table(name = "tests")
public class Test{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private var id: Integer? = null

    private val firstName: String? = null

    private val lastName: String? = null
}