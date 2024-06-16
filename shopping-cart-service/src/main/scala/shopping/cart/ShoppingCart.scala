package shopping.cart

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.serialization.jackson.CborSerializable

import java.time.Instant
import scala.concurrent.duration._

object ShoppingCart {

  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  sealed trait Command extends CborSerializable

  /**
   * A command to add an item to the cart.
   *
   * It replies with `StatusReply[Summary]`, which is sent back to the caller when
   * all the events emitted by this command are successfully persisted.
   */
  final case class AddItem(itemId: String,
                           quantity: Int,
                           replyTo: ActorRef[StatusReply[Summary]]) extends Command

  final case class Checkout(replyTo: ActorRef[StatusReply[Summary]]) extends Command

  final case class Get(replyTo: ActorRef[Summary]) extends Command


  /**
   * Summary of the shopping cart state, used in reply messages.
   */
  final case class Summary(items: Map[String, Int], checkedOut: Boolean) extends CborSerializable


  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  sealed trait Event extends CborSerializable {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int)
    extends Event

  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event

  /**
   * The current state held by the `EventSourcedBehavior`.
   */

  final case class State(items: Map[String, Int], checkoutDate: Option[Instant]) extends CborSerializable {

    def isCheckedOut: Boolean =
      checkoutDate.isDefined

    def checkout(now: Instant): State =
      copy(checkoutDate = Some(now))

    def toSummary: Summary =
      Summary(items, isCheckedOut)

    def hasItem(itemId: String): Boolean =
      items.contains(itemId)

    def isEmpty: Boolean =
      items.isEmpty

    def updateItem(itemId: String, quantity: Int): State = {
      quantity match {
        case 0 => copy(items = items - itemId)
        case _ => copy(items = items + (itemId -> quantity))
      }
    }
  }
  object State {
    val empty =
      State(items = Map.empty, checkoutDate = None)
  }

  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  private def handleCommand(cartId: String,
                            state: State,
                            command: Command): ReplyEffect[Event, State] = {
    // The shopping cart behavior changes if it's checked out or not.
    // The commands are handled differently for each case.
    if (state.isCheckedOut)
      checkedOutShoppingCart(cartId, state, command)
    else
      openShoppingCart(cartId, state, command)
  }

  private def openShoppingCart(cartId: String,
                               state: State,
                               command: Command): ReplyEffect[Event, State] = {
    command match {
      case AddItem(itemId, quantity, replyTo) =>
        if (state.hasItem(itemId))
          Effect.reply(replyTo)(
            StatusReply.Error(
              s"Item '$itemId' was already added to this shopping cart"))
        else if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else
          Effect
            .persist(ItemAdded(cartId, itemId, quantity))
            .thenReply(replyTo) { updatedCart =>
              StatusReply.Success(updatedCart.toSummary)
            }
      case Checkout(replyTo) =>
        if (state.isEmpty)
          Effect.reply(replyTo)(
            StatusReply.Error("Cannot checkout an empty shopping cart"))
        else
          Effect
            .persist(CheckedOut(cartId, Instant.now()))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))
      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
    }
  }

  private def checkedOutShoppingCart(cartId: String,
                                     state: State,
                                     command: Command): ReplyEffect[Event, State] = {
    command match {
      case cmd: AddItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't add an item to an already checked out shopping cart"))
      case cmd: Checkout =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error("Can't checkout already checked out shopping cart"))
    }
  }


  /**
   * The current state held by the `EventSourcedBehavior`.
   */
  private def handleEvent(state: State, event: Event): State = {
    event match {
      case ItemAdded(_, itemId, quantity) => state.updateItem(itemId, quantity)
      case CheckedOut(_, eventTime) => state.checkout(eventTime)
    }
  }

  /**
  * The current state held by the `EventSourcedBehavior`.
  */
  val EntityKey: EntityTypeKey[Command] = EntityTypeKey[Command]("ShoppingCart")

  def init(system: ActorSystem[_]): Unit = {
    ClusterSharding(system).init(
      Entity(EntityKey)(entityContext =>
        ShoppingCart(entityContext.entityId)))
  }

  def apply(cartId: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler = (state, command) => handleCommand(cartId, state, command),
        eventHandler = (state, event) => handleEvent(state, event)
      )
      .withRetention(
        RetentionCriteria.snapshotEvery(numberOfEvents = 100)
      )
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1)
      )
  }
}