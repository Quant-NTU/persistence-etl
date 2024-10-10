package sg.com.quantai.etl.configs

import org.springframework.web.cors.CorsUtils
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity
import org.springframework.security.web.SecurityFilterChain
import org.springframework.http.HttpMethod

@Configuration
@EnableWebSecurity
class SecurityConfiguration () {

    @Throws(Exception::class)
    @Bean
    fun securityFilterChain(http: HttpSecurity) : SecurityFilterChain {
        http
            .cors().and()
            .csrf().disable()
            .authorizeRequests()
            .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
            .requestMatchers(CorsUtils::isPreFlightRequest).permitAll()
            .anyRequest().permitAll()

        return http.build();
    }
}
