package project.injection

import com.google.inject.{Guice, Injector}

object AppInjector {

  val instance: Injector = Guice.createInjector(new AppModule)

}
